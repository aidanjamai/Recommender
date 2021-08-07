using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.ML.Data;
using TestReco.Models;

namespace TestReco
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine(Environment.CurrentDirectory);
            Console.WriteLine(AppDomain.CurrentDomain.BaseDirectory);

            var sqlPath = Environment.CurrentDirectory + @"\Data\Query.sql";

            var sql = await File.ReadAllTextAsync(sqlPath);

            await using var connection = new SqlConnection(@"data source=192.168.1.164\SplendidCRM;initial catalog=SplendidCRM6_CorporateData;persist security info=True;packet size=4096;user id=sa;password=SplendidCRM2005");
            await using var command = new SqlCommand(sql + "select distinct top 500 * from AIQuery order by ID", connection);
            await connection.OpenAsync();

            using var adapter = new SqlDataAdapter(command);
            
            
            var trainingTable = new DataTable();
            adapter.Fill(trainingTable);
            await connection.CloseAsync();

            var testTable = trainingTable.Clone();

            var breakpoint = (int) (trainingTable.Rows.Count * 0.9);
            for (var i = trainingTable.Rows.Count - 1; i >= breakpoint; i--)
            {
                var row = testTable.NewRow();
                row.ItemArray = trainingTable.Rows[i].ItemArray;
                testTable.Rows.Add(row);
                trainingTable.Rows.Remove(trainingTable.Rows[i]);
            }
            
            

            Console.WriteLine(trainingTable.Rows.Count);
            Console.WriteLine(testTable.Rows.Count);


            RecoModel reco = new(trainingTable, "GOOD");

            reco.LoadData();
            reco.Build();

            var fileStream = File.Create(Environment.CurrentDirectory + @"\Data\model.zip");
            reco.Save(fileStream);
            
            reco.Evaluate(testTable);
            reco.PrintMetrics();

            //var input = trainingTable.NewRow();
            //
            //Console.WriteLine("Enter UserId");
            //input["userId"] = int.Parse(Console.ReadLine());
            //
            //Console.WriteLine("Enter MovieId");
            //input["movieId"] = int.Parse(Console.ReadLine());
            
            reco.Predict(testTable.Rows[0]);
            reco.PrintPredictions();
        }
        private static void LoadTestData(DataTable trainingTable, out DataTable testTable) {
            var training = Environment.CurrentDirectory + @"\Data\ratings.csv";

            var columns = new DataColumn[]
            {
                new("userId", typeof(int)),
                new("movieId", typeof(int)),
                //new("rating", typeof(double)),
                new("label", typeof(bool))
            };

            trainingTable.Columns.AddRange(columns);

            testTable = trainingTable.Clone();

            var data = File.ReadAllLines(training)
                .Skip(1)
                .Select(x => x.Split(','))
                .Select(x => new
                {
                    userId = int.Parse(x[0]),
                    movieId = int.Parse(x[1]),
                    rating = double.Parse(x[2]),
                    label = double.Parse(x[2]) > 3,
                    timestamp = int.Parse(x[3])
                })
                .OrderBy(x => x.timestamp)
                .ToList();

        
            for (var i = 0; i < data.Count; i++)
            {
                try
                {
                    var record = data[i];
                    var row = i < data.Count * 0.9
                        ? trainingTable.NewRow()
                        : testTable.NewRow();
                    row["userId"] = record.userId;
                    row["movieId"] = record.movieId;
                    //row["rating"] = record.rating;
                    row["label"] = record.label;
                    if (i < data.Count * 0.9)
                        trainingTable.Rows.Add(row);
                    else
                        testTable.Rows.Add(row);
                }
                catch (Exception e)
                {
                    Console.WriteLine(i);
                    throw;
                }
            }
        }
    }
}