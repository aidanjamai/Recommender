using System;
using System.IO;
using TestReco.Models;

namespace TestReco
{
    class Program
    {
        static void Main(string[] args)
        {
            RecoModel reco = new();
            var training = @"C:\Users\AIdan\Desktop\test\TestReco\TestReco\Data\ratings_train.csv";
            var test = @"C:\Users\AIdan\Desktop\test\TestReco\TestReco\Data\ratings_test.csv";
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
