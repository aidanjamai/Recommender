using Microsoft.ML;
using Microsoft.ML.Data;
using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.ML.Trainers;
using Microsoft.ML.Transforms;

namespace TestReco.Models
{
    internal class RecoModel
    {
        private readonly MLContext _mlContext = new MLContext();

        private IDataView _allData;

        private ITransformer _model;

        public const int _ratingTreshold = 3;

        private CalibratedBinaryClassificationMetrics _metrics;

        private OutputModel _results;

        private readonly string _labelColumn;
        private DataTable _dataTable;
        private Type _generatedType;

        private static string WholeTrainingDataPath = Environment.CurrentDirectory + @"\Data\ratings.csv";
        private static string SplitTrainingDataPath = Environment.CurrentDirectory + @"\Data\ratings_train.csv";
        private static string TestDataPath = Environment.CurrentDirectory + @"\Data\ratings_test.csv";
        private static string ModelPath = Environment.CurrentDirectory + @"\Data\model.zip";

        public RecoModel(DataTable dataTable, string labelColumn)
        {
            _dataTable = dataTable;
            _labelColumn = labelColumn;
            _generatedType = GenerateTypeFromDataTable(dataTable);
        }

        public void LoadData()
        {
            _allData = ProcessDataTable(_dataTable);
        }

        private IDataView ProcessDataTable(DataTable table)
        {
            var listType = typeof(List<>).MakeGenericType(_generatedType);

            var list = Activator.CreateInstance(listType);

            for (var i = 0; i < table.Rows.Count; i++)
            {
                var row = table.Rows[i];
                var record = Activator.CreateInstance(_generatedType);
                for (var j = 0; j < table.Columns.Count; j++)
                {
                    var column = table.Columns[j];
                    var property = _generatedType.GetField(column.ColumnName);
                    var value = row[column.ColumnName];
                    if (value is DBNull) continue;
                    if (value is Guid)
                        property.SetValue(record, value.ToString());
                    else
                        property.SetValue(record, value);
                }

                listType
                    .GetMethod("Add")
                    .Invoke(list, new[] {record});
            }

            var schemaDefinition = SchemaDefinition.Create(_generatedType);

            return typeof(DataOperationsCatalog)
                .GetMethods()
                .First(x => x.Name == nameof(MLContext.Data.LoadFromEnumerable) && x.GetParameters()[1].IsOptional)
                .MakeGenericMethod(_generatedType)
                .Invoke(_mlContext.Data, new object[]
                {
                    list,
                    schemaDefinition
                }) as IDataView;
        }

        private Type GenerateTypeFromDataTable(DataTable table)
        {
            var assemblyName = new AssemblyName(Assembly.GetAssembly(typeof(RecoModel))?.FullName ?? "" + "Dynamic");
            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
            var module = assemblyBuilder.DefineDynamicModule(assemblyName.Name + ".dll");
            var typeBuilder = module.DefineType("DynamicInputModel", TypeAttributes.Public);
            for (var i = 0; i < table.Columns.Count; i++)
            {
                var column = table.Columns[i];
                var columnType = column.DataType == typeof(Guid) 
                    ? typeof(string) 
                    : column.DataType;
                typeBuilder.DefineField(column.ColumnName, columnType, FieldAttributes.Public);
            }

            return typeBuilder.CreateType();
        }

        public void Build()
        {
            Console.WriteLine("Building");

            var options = new FieldAwareFactorizationMachineTrainer.Options
            {
                Shuffle = false,
                NumberOfIterations = 100,
                LatentDimension = 10,
                LearningRate = .001f,
                LabelColumnName = _labelColumn,
                FeatureColumnName = "Features"
            };

            IEstimator<ITransformer> pipeline = null;
            var featureColumnNames = new List<string>();
            for (var i = 0; i < _dataTable.Columns.Count; i++)
            {
                var columnName = _dataTable.Columns[i].ColumnName;
                if (columnName == _labelColumn)
                    continue;
                var featureColumnName = columnName + "OneHot";
                featureColumnNames.Add(featureColumnName);
                var estimator = _mlContext.Transforms.Categorical.OneHotEncoding(featureColumnName, columnName);
                if (i == 0)
                    pipeline = estimator;
                else
                    pipeline = pipeline.Append(estimator);
            }

            pipeline = pipeline
                .Append(_mlContext.Transforms.Concatenate("Features", featureColumnNames.ToArray()))
                .Append(_mlContext.BinaryClassification.Trainers.FieldAwareFactorizationMachine(options));

            Console.WriteLine("Training");

            _model = pipeline.Fit(_allData);

            //var metrics = _mlContext.BinaryClassification.CrossValidate(_allData,
            //    pipeline, 5, _labelColumn);
            //
            //var jsonOptions = new JsonSerializerOptions()
            //{
            //    ReferenceHandler = ReferenceHandler.Preserve
            //};
            //foreach (var metric in metrics)
            //{
            //    Console.WriteLine(JsonSerializer.Serialize(metric.Metrics));
            //}
        }


        public CalibratedBinaryClassificationMetrics Evaluate(DataTable table)
        {
            Console.WriteLine("Evaluating Model using test data");
            Console.WriteLine();
            var testData = ProcessDataTable(table);

            var scoredData = _model.Transform(testData);
            _metrics = _mlContext.BinaryClassification.Evaluate(
                data: scoredData,
                labelColumnName: _labelColumn,
                scoreColumnName: "Score",
                predictedLabelColumnName: "PredictedLabel");


            // Place a breakpoint here to inspect the quality metrics.
            return _metrics;
        }

        public OutputModel Predict(DataRow record)
        {
            Console.WriteLine("Predicting output using trained model and Input data");
            Console.WriteLine();

            var engine = typeof(ModelOperationsCatalog)
                .GetMethods()
                .First(x => x.Name == nameof(ModelOperationsCatalog.CreatePredictionEngine) &&
                            x.GetParameters()[1].IsOptional)
                .MakeGenericMethod(_generatedType, typeof(OutputModel))
                .Invoke(_mlContext.Model, new object[]
                {
                    _model,
                    true,
                    null,
                    null
                });

            var data = Activator.CreateInstance(_generatedType);

            for (var i = 0; i < record.Table.Columns.Count; i++)
            {
                var column = record.Table.Columns[i];
                if (column.ColumnName == _labelColumn)
                    continue;
                var property = _generatedType.GetField(column.ColumnName);
                var value = record[column.ColumnName];
                if (value is DBNull)
                    continue;
                property?.SetValue(data, value is Guid
                    ? value.ToString()
                    : value);
            }

            _results = engine.GetType()
                .GetMethods()
                .First(x => x.Name == "Predict" && x.ReturnType == typeof(OutputModel)).Invoke(engine, new[]
                {
                    data
                }) as OutputModel;

            return _results;
        }

        public void Save(Stream stream)
        {
            Console.WriteLine("Saving Model");
            Console.WriteLine();

            _mlContext.Model.Save(_model, inputSchema: _allData.Schema, stream);
        }

        public void PrintMetrics()
        {
            Console.WriteLine("Printing Metrics");
            Console.WriteLine("Evaluation Metrics: acc:" + _metrics.Accuracy +
                              " AreaUnderRocCurve(AUC):" + _metrics.AreaUnderRocCurve);
            Console.WriteLine();
        }

        public void PrintPredictions()
        {
            Console.WriteLine("Printing Predictions");
            Console.WriteLine($"Score:{Sigmoid(_results.Score)} and Label {_results.PredictedLabel}");
            Console.WriteLine();
        }

        public static float Sigmoid(float x)
        {
            return (float) (100 / (1 + Math.Exp(-x)));
        }
    }
}