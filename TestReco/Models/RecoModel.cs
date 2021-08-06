using Microsoft.ML;
using Microsoft.ML.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ML.Trainers;

namespace TestReco.Models
{
    internal class RecoModel
    {
        private readonly MLContext _mlContext = new MLContext();

        private IDataView _allData;

        private ITransformer _model;

        public const int _ratingTreshold = 3;

        private PredictionEngine<InputModel, OutputModel> _predictionEngine;

        private CalibratedBinaryClassificationMetrics _metrics;

        private OutputModel _results;

        private static string WholeTrainingDataPath = Environment.CurrentDirectory +  @"\Data\ratings.csv";
        private static string SplitTrainingDataPath = Environment.CurrentDirectory +  @"\Data\ratings_train.csv";
        private static string TestDataPath = Environment.CurrentDirectory +  @"\Data\ratings_test.csv";
        private static string ModelPath = Environment.CurrentDirectory +  @"\Data\model.zip";



        public IEnumerable<InputModel> LoadData()
        {
            // Populating an IDataView from an IEnumerable.
            /*var trainingDataView = _mlContext.Data.LoadFromTextFile<InputModel>(path: SplitTrainingDataPath, hasHeader: true, separatorChar: ',');
            trainingDataView = _mlContext.Data.Cache(trainingDataView);*/
            var data = File.ReadAllLines(SplitTrainingDataPath)
                .Skip(1)
               .Select(x => x.Split(','))
               .Select(x => new InputModel
               {
                   UserId = x[0],
                   MovieId = x[1],
                   Label = bool.Parse(x[2])

               });

            _allData = _mlContext.Data.LoadFromEnumerable(data);

            return _mlContext.Data.CreateEnumerable<InputModel>(_allData, reuseRowObject: false);
        }

        public void Build()
        {
            Console.WriteLine("Building");
            Console.WriteLine();



            var options = new FieldAwareFactorizationMachineTrainer.Options
            {

                Shuffle = false,
                NumberOfIterations = 100,
                LatentDimension = 10,
                LearningRate = .001f,
            };

            var pipeline = _mlContext.Transforms.Categorical.OneHotEncoding("UserIdOneHot", "UserId")
                .Append(_mlContext.Transforms.Categorical.OneHotEncoding("MovieIdOneHot", "MovieId"))
                .Append(_mlContext.Transforms.Concatenate("Features", "UserIdOneHot", "MovieIdOneHot"))
                .Append(_mlContext.BinaryClassification.Trainers.FieldAwareFactorizationMachine(new string[] { "Features" }))
                .Append(_mlContext.BinaryClassification.Trainers.FieldAwareFactorizationMachine(options));

            var trainingData = _allData;
            //trainingData = _mlContext.Data.TakeRows(trainingData, 450);

            // Place a breakpoint here to peek the training data.
            //var preview = pipeline.Preview(trainingData, maxRows: 10);

            _model = pipeline.Fit(trainingData);
            Console.WriteLine("Training");
            Console.WriteLine();

        }

        public CalibratedBinaryClassificationMetrics Evaluate()
        {
            /*var testData = _mlContext.Data.ShuffleRows(_allData);
            //testData = _mlContext.Data.TakeRows(testData, 100);*/
            Console.WriteLine("Evaluating Model using test data");
            Console.WriteLine();
            var testData = _mlContext.Data.LoadFromTextFile<InputModel>(path: TestDataPath, hasHeader: true, separatorChar: ',');

            //var prediction = _model.Transform(testDataView);

            var scoredData = _model.Transform(testData);
            _metrics = _mlContext.BinaryClassification.Evaluate(
                data: scoredData,
                labelColumnName: "Label",
                scoreColumnName: "Score",
                predictedLabelColumnName: "PredictedLabel");

            

            // Place a breakpoint here to inspect the quality metrics.
            return _metrics;
        }

        public OutputModel Predict(InputModel recommendationData)
        {
            Console.WriteLine("Predicting output using trained model and Input data");
            Console.WriteLine();
            _predictionEngine = _mlContext.Model.CreatePredictionEngine<InputModel, OutputModel>(_model);

            if (_predictionEngine == null)
            {
                return null;
            }
            _results = _predictionEngine.Predict(recommendationData);
            // Single prediction
            return _results;
        }

        public void Save()
        {
            Console.WriteLine("Saving Model");
            Console.WriteLine();

            _mlContext.Model.Save(_model, inputSchema: null, filePath: ModelPath);
        }

        public void PrintMetrics()
        {
            Console.WriteLine("Printing Metrics");
            Console.WriteLine("Evaluation Metrics: acc:" + Math.Round(_metrics.Accuracy, 2) + " AreaUnderRocCurve(AUC):" + Math.Round(_metrics.AreaUnderRocCurve, 2));
            Console.WriteLine();
        }

        public void PrintPredictions()
        {
            Console.WriteLine("Printing Predictions");
            Console.WriteLine($"Score:{Sigmoid(_results.Score)} and Label {_results.PredictedLabel}");
            Console.WriteLine();

        }

        public void SplitData()
        {
            Console.WriteLine("Splitting training data into test data and train data");
            string[] dataset = File.ReadAllLines(WholeTrainingDataPath);
            string[] new_dataset = new string[dataset.Length];
            new_dataset[0] = dataset[0];
            for (int i = 1; i < dataset.Length; i++)
            {
                var line = dataset[i];
                var lineSplit = line.Split(',');
                
                var rating = double.Parse(lineSplit[2]) > _ratingTreshold;
                
                lineSplit[2] = rating.ToString();
                var new_line = string.Join(',', lineSplit);
                new_dataset[i] = new_line;
            }
            dataset = new_dataset;
            var numLines = dataset.Length;
            var body = dataset.Skip(1);
            var sorted = body.Select(line => new { SortKey = Int32.Parse(line.Split(',')[3]), Line = line })
                             .OrderBy(x => x.SortKey)
                             .Select(x => x.Line);
            File.WriteAllLines(SplitTrainingDataPath, dataset.Take(1).Concat(sorted.Take((int)(numLines * 0.9))));
            File.WriteAllLines(TestDataPath, dataset.Take(1).Concat(sorted.TakeLast((int)(numLines * 0.1))));
        }

        public static float Sigmoid(float x)
        {
            return (float)(100 / (1 + Math.Exp(-x)));
        }


    }




}
