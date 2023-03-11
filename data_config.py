toplevel_cols = [
'PartitionKey',
# 'Program',
# 'AssessmentDate',
# 'AssessmentType',
'SurveyData'

]

survey_datacols = [
'PartitionKey',
#'RowKey',
'Program',
#'Staff',
'AssessmentDate',
'SurveyName',
'ClientType',
'IndigenousStatus',
#'PDC',

'HaveYouEverInjected'
]
survey_datacols_preregex = [
   '.*_Score$'
]
survey_datacols_postregex = [
   '^Past4Wk.*'
]

cols = [
'PartitionKey',
'RowKey',
'Program',
'Staff',
'AssessmentDate',
'SurveyName',
'ClientType',
'IndigenousStatus',
'PDC.PDCSubstanceOrGambling',
'PDC.PDCMethodOfUse',
'PDC.PDCDaysInLast28',
'PDC.PDCUnits',
'PDC.PDCHowMuchPerOccasion',

'HaveYouEverInjected',
'SDSIsAODUseOutOfControl',
'SDSDoesMissingFixMakeAnxious',
'SDSHowMuchDoYouWorryAboutAODUse',
'SDSDoYouWishToStop',
'SDSHowDifficultToStopOrGoWithout'
]

rename_cols = {
'PDC.PDCSubstanceOrGambling'		: 'PDCSubstanceOrGambling',
'PDC.PDCMethodOfUse'		: 'PDCMethodOfUse',
'PDC.PDCDaysInLast28'		: 'PDCDaysInLast28',
'PDC.PDCUnits'		: 'PDCUnits',
'PDC.PDCHowMuchPerOccasion'		: 'PDCHowMuchPerOccasion'
}

data_types = {
  'PartitionKey': 'string',
'RowKey': 'string',
'Program': 'string',
'Staff': 'string',
'AssessmentDate': 'date',
'SurveyName': 'string',
'ClientType': 'string',
'IndigenousStatus': 'string',
'PDC.PDCSubstanceOrGambling': 'string',
'PDC.PDCMethodOfUse': 'string',
'PDC.PDCDaysInLast28': 'int',
'PDC.PDCUnits': 'string',
'PDC.PDCHowMuchPerOccasion': 'float',

'HaveYouEverInjected': 'string',
'SDSIsAODUseOutOfControl': 'int',
'SDSDoesMissingFixMakeAnxious': 'int',
'SDSHowMuchDoYouWorryAboutAODUse': 'int',
'SDSDoYouWishToStop': 'int',
'SDSHowDifficultToStopOrGoWithout': 'int',
}