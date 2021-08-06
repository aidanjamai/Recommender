using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.ML.Data;
using TestReco.Models;

namespace TestReco
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(Environment.CurrentDirectory);
            Console.WriteLine(AppDomain.CurrentDomain.BaseDirectory);

            var training = Environment.CurrentDirectory + @"\Data\ratings.csv";
            var test = Environment.CurrentDirectory + @"\Data\ratings_test.csv";

            var columns = new DataColumn[]
            {
                new("userId", typeof(int)),
                new("movieId", typeof(int)),
                //new("rating", typeof(double)),
                new("label", typeof(bool))
            };

            var trainingTable = new DataTable();
            trainingTable.Columns.AddRange(columns);

            var testTable = trainingTable.Clone();

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

            Console.WriteLine(trainingTable.Rows.Count);
            Console.WriteLine(testTable.Rows.Count);


            RecoModel reco = new(trainingTable, "label");

            reco.LoadData();
            reco.Build();
            reco.Save();
            reco.Evaluate(testTable);
            reco.PrintMetrics();

            var input = trainingTable.NewRow();

            Console.WriteLine("Enter UserId");
            input["userId"] = int.Parse(Console.ReadLine());

            Console.WriteLine("Enter MovieId");
            input["movieId"] = int.Parse(Console.ReadLine());
            
            reco.Predict(input);
            reco.PrintPredictions();
        }
    }
}