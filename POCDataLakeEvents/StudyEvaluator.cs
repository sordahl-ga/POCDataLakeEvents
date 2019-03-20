using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using System.Globalization;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Linq;

namespace POCDataLakeEvents
{
    public class StudyModel
    {
        public bool isEligable { get; set; }
        public string patId { get; set; }
        public int ageYearsAtStudyDate { get; set; }
        public DateTime birthDate { get; set; }
        public DateTime studyDate { get; set; }
        public string a1cComparator { get; set; }
        public float a1cValue { get; set; }
        public DateTime a1cLastTestDate { get; set; }
        public string a1cLastTestId { get; set; }
    }
    public static class StudyEvaluator
    {
        [FunctionName("StudyEvaluator")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            [CosmosDB(
                databaseName:"hl7json",
                collectionName :"messages",
                ConnectionStringSetting = "CosmosDBConnectionHL7POC")] DocumentClient client,
            ILogger log)
        {
            log.LogInformation("Study Evaluator trigger fired");
            try
            {
                bool dateelig = false;
                bool a1celig = false;
                var retVal = new StudyModel();
                string studydate = req.Query["dateofstudy"];
                string daysback = req.Query["daysback"];
                string a1cstring = req.Query["a1cstring"];
                string minage = req.Query["minage"];
                string maxage = req.Query["maxage"];
                string maxa1c = req.Query["maxa1c"];
                if (maxa1c == null) maxa1c = "8.0";
                if (minage == null) minage = "18";
                if (maxage == null) maxage = "75";
                if (a1cstring == null) a1cstring = "A1C";
                if (studydate == null) studydate = DateTime.Now.ToString("yyyyMMdd");
                if (daysback == null) daysback = "180";
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                var obj = JObject.Parse(requestBody);
                string patid = (string)obj["hl7message"]["PID"]["PID.3"]["PID.3.1"];
                retVal.patId = patid;
                //Criteria 1: Age 18 to 75 as of trial enrollment date
                DateTime dts = ConvertHL7Date(studydate);
                DateTime bd = ConvertHL7Date((string)obj["hl7message"]["PID"]["PID.7"]);
                int age = dts.Year - bd.Year;
                if (dts.Month < bd.Month ||
                   ((dts.Month == bd.Month) && (dts.Day < bd.Day)))
                {
                    age--;
                }
                retVal.birthDate = bd;
                retVal.studyDate = dts;
                retVal.ageYearsAtStudyDate = age;
                int iMinAge = int.Parse(minage);
                int iMaxAge = int.Parse(maxage);
                if (age >= iMinAge && age <= iMaxAge) dateelig = true;
                //Criteria 2&3 A1C Lab Test obtained in past 6 months and less than 8
                var options = new FeedOptions
                {
                    MaxItemCount = 100,
                    EnableCrossPartitionQuery = true,
                    MaxDegreeOfParallelism = 10,
                    MaxBufferedItemCount = 100
                };
                string a1cquery = "select c as content from c where c['hl7message']['MSH']['MSH.9']['MSH.9.1']='ORU' and c['hl7message']['PID']['PID.3']['PID.3.1']='~patid~' and CONTAINS(c['hl7message']['OBX']['OBX.3']['OBX.3.2'],'~a1cstring~') order by c['hl7message']['MSH']['MSH.7'] desc".Replace("~patid~", patid).Replace("~a1cstring~",a1cstring);
                var collection = UriFactory.CreateDocumentCollectionUri(System.Environment.GetEnvironmentVariable("DBNAMEHL7"), System.Environment.GetEnvironmentVariable("COLLECTIONHL7"));
                var docq = client.CreateDocumentQuery<Document>(collection, a1cquery, options).AsDocumentQuery();
                var rslt = await docq.ExecuteNextAsync<Document>();
                foreach (Document doc in rslt)
                {

                    var docobj = (JObject)(dynamic)doc;
                    string msgid = (string)docobj["content"]["id"];
                    string rtype = (string)docobj["content"]["hl7message"]["OBX"]["OBX.2"];
                    DateTime testdate = ConvertHL7Date((string)docobj["content"]["hl7message"]["OBX"]["OBX.14"]);
                    int iDaysAgo = int.Parse(daysback) * -1;
                    DateTime daysAgo = dts.AddDays(iDaysAgo);
                    if (testdate >= daysAgo)
                    {
                        float fma1c = float.Parse(maxa1c);
                        if (rtype.Equals("SN"))
                        {
                            string comp = (string)docobj["content"]["hl7message"]["OBX"]["OBX.5"]["OBX.5.1"];
                            float num = float.Parse((string)docobj["content"]["hl7message"]["OBX"]["OBX.5"]["OBX.5.2"]);
                            if ((comp.Equals("<") || comp.Equals("=")) && (num > 0 && num < fma1c))
                            {
                                a1celig = true;
                                retVal.a1cComparator = comp;
                                retVal.a1cValue = num;
                                retVal.a1cLastTestDate = testdate;
                                retVal.a1cLastTestId = msgid;
                                break;
                            }
                        }
                        else
                        {
                            float num = float.Parse((string)docobj["content"]["hl7message"]["OBX"]["OBX.5"]);
                            if (num > 0 && num < fma1c)
                            {
                                a1celig = true;
                                retVal.a1cComparator = "=";
                                retVal.a1cValue = num;
                                retVal.a1cLastTestDate = testdate;
                                retVal.a1cLastTestId = msgid;
                                break;
                            }
                        }
                    }

                }
                if (dateelig && a1celig)
                {
                    retVal.isEligable = true;
                    return new JsonResult(retVal);
                }
                var notelig = new StudyModel { isEligable = false };
                return new JsonResult(notelig);
            }
            catch(Exception e1)
            {
                log.LogError("Unable to evaulate Study trigger:" + e1.Message, e1);
                log.LogError("Trace: " + e1.StackTrace);
                var notelig = new StudyModel { isEligable = false };
                return new JsonResult(notelig);
            }
            
        }
        private static DateTime ConvertHL7Date(string hl7date)
        {
            if (hl7date.Length > 8) hl7date = hl7date.Substring(0, 8);
            DateTime dt = DateTime.ParseExact(hl7date, "yyyyMMdd",
                                  CultureInfo.InvariantCulture);
            return dt;

        }
    }
}
