using System;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using System.Globalization;
using System.Threading;
using Microsoft.SqlServer.Dts.Pipeline.Design;

namespace SSISCustomComponents
{
    [DtsPipelineComponent(DisplayName = "INITCAP"
                        , Description = "INITCAP Component for Dataflow Task- Developed by Tejpal Baghel"
                        , ComponentType = ComponentType.Transform
                        , IconResource = "Icon.ico")]

    public class INITCAP : PipelineComponent
    {
        private int[] inputBufferColindex;
        private int[] outputBufferColindex;



        #region ProvideComponentProperties

        public override void ProvideComponentProperties()
        {
            ComponentMetaData.Name = "INITCAP";
            ComponentMetaData.Description = "INITCAP Component for Dataflow Task- Developed by Tejpal Baghel";
            ComponentMetaData.ContactInfo = "Tejpal.Baghel@3PillarGlobal.com";

            RemoveAllInputsOutputsAndCustomProperties();

            IDTSInput100 inputObject = ComponentMetaData.InputCollection.New();
            inputObject.Name = "InputToINITCAP";
            inputObject.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;

            IDTSOutput100 outputObject = ComponentMetaData.OutputCollection.New();
            outputObject.Name = "OutputFromINITCAP";
            outputObject.SynchronousInputID = inputObject.ID; //Synchronous Transform           

            IDTSCustomProperty100 TextProcessing = ComponentMetaData.CustomPropertyCollection.New();
            TextProcessing.Name = "EnableTextProcessing";
            TextProcessing.Description = "Enable Text Processing";
            TextProcessing.Value = "True";

            IDTSCustomProperty100 AlwaysUpperCase = ComponentMetaData.CustomPropertyCollection.New();
            AlwaysUpperCase.Name = "UpperCase";
            AlwaysUpperCase.Description = "Always Upper Case";
            AlwaysUpperCase.Value = "IND,US,UK";

            IDTSCustomProperty100 AlwaysLowerCase = ComponentMetaData.CustomPropertyCollection.New();
            AlwaysLowerCase.Name = "LowerCase";
            AlwaysLowerCase.Description = "Always Lower Case";
            AlwaysLowerCase.Value = "in,out,for,and,or,not,yes,no";

            IDTSCustomProperty100 MixedCaseInput = ComponentMetaData.CustomPropertyCollection.New();
            MixedCaseInput.Name = "MixedCase";
            MixedCaseInput.Description = "Mixed Case";
            MixedCaseInput.Value = "Po Box|PO Box,hod|HoD";

            IDTSCustomProperty100 ColumnsPrefix = ComponentMetaData.CustomPropertyCollection.New();
            ColumnsPrefix.Name = "ColumnsPrefix";
            ColumnsPrefix.Description = "ColumnsPrefix";
            ColumnsPrefix.Value = "INITCAP_";

            AddErrorOutput("ErrorFromINITCAP", inputObject.ID, outputObject.ExclusionGroup);
        }

        public override DTSValidationStatus Validate()
        {
            IDTSInput100 input = ComponentMetaData.InputCollection[0];
            string errorMsg = "Wrong datatype specified for {0}. It accepts only DT_STR or DT_WSTR";

            for (int x = 0; x < input.InputColumnCollection.Count; x++)
            {
                if (!(input.InputColumnCollection[x].DataType == DataType.DT_STR
                   || input.InputColumnCollection[x].DataType == DataType.DT_WSTR))
                {
                    Logger(String.Format(errorMsg, input.InputColumnCollection[x].Name));
                    return DTSValidationStatus.VS_ISCORRUPT;
                }
            }

            IDTSOutput100 output = ComponentMetaData.OutputCollection["OutputFromINITCAP"];
            foreach (IDTSInputColumn100 inputColumn in input.InputColumnCollection)
            {
                bool IsPresent = false;
                foreach (IDTSOutputColumn100 outputColumn in output.OutputColumnCollection)
                {

                    if (outputColumn.Name == ComponentMetaData.CustomPropertyCollection["ColumnsPrefix"].Value.ToString() + inputColumn.Name)
                    {
                        IsPresent = true;
                    }
                }

                if (!IsPresent)
                {
                    IDTSOutputColumn100 outputcol = output.OutputColumnCollection.New();
                    outputcol.Name = ComponentMetaData.CustomPropertyCollection["ColumnsPrefix"].Value.ToString() + inputColumn.Name;
                    outputcol.Description = String.Format("{0} contains valid data", inputColumn.Name);
                    outputcol.SetDataTypeProperties(inputColumn.DataType, inputColumn.Length, 0, 0, inputColumn.CodePage);
                }
            }

            return DTSValidationStatus.VS_ISVALID;
        }

        public override void ReinitializeMetaData()
        {
            ComponentMetaData.RemoveInvalidInputColumns();
            ReinitializeMetaData();
        }
     
        public override IDTSOutputColumn100 InsertOutputColumnAt(int outputID, int outputColumnIndex, string name, string description)
        {
            Logger(string.Format("Fail to add output column name to {0} ", ComponentMetaData.Name));
            throw new Exception(string.Format("Fail to add output column name to {0} ", ComponentMetaData.Name), null);
        }
        #endregion


        #region Runtime methods
        private void Logger(string MessageText)
        {
            bool cancel = false;
            this.ComponentMetaData.FireError(0, this.ComponentMetaData.Name, MessageText, "", 0, out cancel);
        }
        public override void PreExecute()
        {

            IDTSInput100 input = ComponentMetaData.InputCollection[0];
            inputBufferColindex = new int[input.InputColumnCollection.Count];

            Enumerable
                .Range(0, input.InputColumnCollection.Count)
                .ToList()
                .ForEach(i =>
                {
                    IDTSInputColumn100 inputCol = input.InputColumnCollection[i];
                    inputBufferColindex[i] = BufferManager
                                               .FindColumnByLineageID(input.Buffer, inputCol.LineageID);
                });


            IDTSOutput100 output = ComponentMetaData.OutputCollection["OutputFromINITCAP"];
            outputBufferColindex = new int[output.OutputColumnCollection.Count];

            Enumerable
                .Range(0, input.InputColumnCollection.Count)
                .ToList()
                .ForEach(i =>
                {
                    IDTSOutputColumn100 outputCol = output.OutputColumnCollection[i];
                    outputBufferColindex[i] = BufferManager
                                                .FindColumnByLineageID(input.Buffer, outputCol.LineageID);
                });
        }
        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            IDTSInput100 input = ComponentMetaData.InputCollection.GetObjectByID(inputID);
            if (!buffer.EndOfRowset)
            {
                while (buffer.NextRow())
                {
                    for (int x = 0; x < inputBufferColindex.Length; x++)
                    {
                        string ProperCaseData = "";
                        try
                        {
                            ProperCaseData = InitCap_ProcessText(buffer.GetString(inputBufferColindex[x]));
                            buffer.SetString(outputBufferColindex[x], ProperCaseData);
                        }
                        catch (Exception ex)
                        {
                            //buffer.DirectErrorRow(outputBufferColindex[x], -1, inputBufferColindex[x]);
                        }
                    }
                }
            }
        }
        private string InitCap_ProcessText(string InputText)
        {
            string ProperCaseText = InputText.ToString();
            CultureInfo cultureInfo = Thread.CurrentThread.CurrentCulture;
            TextInfo textInfo = cultureInfo.TextInfo;
            ProperCaseText = textInfo.ToTitleCase(ProperCaseText.ToLower());
            if (Convert.ToBoolean(ComponentMetaData.CustomPropertyCollection["EnableTextProcessing"].Value.ToString()) == true)
            {
                ProperCaseText = Exceptional_TextProcessing(ProperCaseText);
            }
            return ProperCaseText.Trim();
        }
        private string Exceptional_TextProcessing(string ProperCaseText)
        {
            CultureInfo cultureInfo = Thread.CurrentThread.CurrentCulture;
            TextInfo textInfo = cultureInfo.TextInfo;
            string[] ExceptionalCase;
            string[] ExceptionalCase_Mixed;
            string TempString;
            ProperCaseText = " " + ProperCaseText + " ";

            // tranformation exceptions: words to be lowercased 
            ExceptionalCase = textInfo.ToTitleCase(ComponentMetaData.CustomPropertyCollection["LowerCase"].Value.ToString().ToLower()).Split(',');
            foreach (string myValue in ExceptionalCase)
            {
                TempString = " " + myValue.Trim() + " ";
                if (ProperCaseText.Contains(myValue))
                {
                    ProperCaseText = ProperCaseText.Replace(TempString, TempString.ToLower());
                }
            }

            // tranformation exceptions: words to be UPPERCASED 
            ExceptionalCase = textInfo.ToTitleCase(ComponentMetaData.CustomPropertyCollection["UpperCase"].Value.ToString().ToLower()).Split(',');
            foreach (string myValue in ExceptionalCase)
            {
                TempString = " " + myValue.Trim() + " ";
                if (ProperCaseText.Contains(TempString))
                {
                    ProperCaseText = ProperCaseText.Replace(TempString, TempString.ToUpper());
                }
            }

            // tranformation exceptions: words to be Mixed CASED
            ExceptionalCase = ComponentMetaData.CustomPropertyCollection["MixedCase"].Value.ToString().Split(',');
            foreach (string myValue in ExceptionalCase)
            {
                ExceptionalCase_Mixed = myValue.Split('|');
                TempString = " " + textInfo.ToTitleCase(ExceptionalCase_Mixed[0].ToLower()) + " ";
                if (ProperCaseText.Contains(TempString))
                {
                    ProperCaseText = ProperCaseText.Replace(TempString, ExceptionalCase_Mixed[1].ToString());
                }
            }
            return ProperCaseText.Trim();
        }
        #endregion Run Time Methods
    }
}