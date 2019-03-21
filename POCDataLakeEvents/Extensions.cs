using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace POCDataLakeEvents
{
    public class PagedResults<T>
    {
        public PagedResults()
        {
            Results = new List<T>();
        }
        /// <summary>
        /// Continuation Token for DocumentDB
        /// </summary>
        public string ContinuationToken { get; set; }

        /// <summary>
        /// Results
        /// </summary>
        public List<T> Results { get; set; }
    }

    public static class Extensions
    {
        public static async Task<PagedResults<T>> ToPagedResults<T>(this IQueryable<T> source)
        {
            var documentQuery = source.AsDocumentQuery();
            var results = new PagedResults<T>();

            try
            {
                var queryResult = await documentQuery.ExecuteNextAsync<T>();
                if (!queryResult.Any())
                {
                    return results;
                }
                results.ContinuationToken = queryResult.ResponseContinuation;
                results.Results.AddRange(queryResult);
            }
            catch (Exception e2)
            {
                //documentQuery.ExecuteNextAsync throws an Exception if there are no results
                return results;
            }

            return results;
        }

    }
}
