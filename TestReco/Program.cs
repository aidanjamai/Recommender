using System;
using System.IO;
using TestReco.Models;

namespace TestReco
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(Environment.CurrentDirectory);
            Console.WriteLine(AppDomain.CurrentDomain.BaseDirectory);
            RecoModel reco = new();
            var training =  Environment.CurrentDirectory +  @"\Data\ratings_train.csv";
            var test = Environment.CurrentDirectory +  @"\Data\ratings_test.csv";
            if (!File.Exists(training) && !File.Exists(test))
            {
                reco.SplitData();
            }
            else
            {
                reco.LoadData();
                reco.Build();
                reco.Save();
                reco.Evaluate();
                reco.PrintMetrics();
            }

            InputModel input = new();

            Console.WriteLine("Enter UserId");
            input.UserId = Console.ReadLine();

            Console.WriteLine("Enter MovieId");
            input.MovieId = Console.ReadLine();

            reco.Predict(input);
            reco.PrintPredictions();
        }
    }
}