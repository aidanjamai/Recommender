using Microsoft.ML.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestReco.Models
{
    class InputModel
    {
        [LoadColumn(0)]
        public string UserId;
        [LoadColumn(1)]
        public string MovieId;
        [LoadColumn(2)]
        public bool Label;
    }
}
