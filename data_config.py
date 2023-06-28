from pandas.api.types import CategoricalDtype

keep_parent_fields = ['Program']

# range
remove_if_under_threshold = {
     'PDCHowMuchPerOccasion' : {
          'Other': 2 # if under 2 percent of dataset, delete records
     }
}
category_yes_no = ['Yes', 'No']

# TODO Fix:
# Past4WkBeenArrested :'No', 'Yes', 'Yes - please provide details'

# df.Past4WkAodRisks[df.Past4WkAodRisks.notna()]
# 3       [Driving with drugs and/or alcohol in your sys...
# 8                    [Using more than one drug at a time]
# 311       [Memory Loss, Using Alone, Violence / Assault]
# 7312                                        [Using Alone]
# 7314    [Using Alone, Driving with drugs and/or alcoho...


category_feel_safe = ['Yes - Completely safe',
       'Mostly safe. Sometimes feel threatened',
       'Often feel unsafe / Occasionally experience violence',
       'Never feel safe / Constantly exposed to violence',
       'Never feel safe. Constantly exposed to violence',
       'Often feel unsafe. Occasionally experience violence']

category_notatall_to_daily = CategoricalDtype(['Not at all',  'Less than weekly', 'Once or twice per week',
       'Three or four times per week', 'Daily or almost daily'], ordered=True)

# HowSatisfiedWithProgress
category_notatall_to_extremely = CategoricalDtype([
        'Not at all ' , 'Slightly' , 'Moderately'
        , 'Considerably' , 'Extremely'], ordered=True)


predef_categories = {
    'Past4WkUseLedToProblemsWithFamilyFriend':category_notatall_to_daily,
  'Past4WkHowOftenIllegalActivities':       category_notatall_to_daily,
  'Past4WkHowOftenMentalHealthCausedProblems': category_notatall_to_daily,
  'Past4WkHowOftenPhysicalHealthCausedProblems': category_notatall_to_daily, 
  'Past4WkDifficultyFindingHousing': category_notatall_to_daily,
  'HowSatisfiedWithProgress': category_notatall_to_extremely,
  # 'DoYouFeelSafeWhereYouLive': category_feel_safe,
  'Past4WkHadCaregivingResponsibilities': category_yes_no,
  'Past4WkBeenHospCallAmbulance': category_yes_no,
  'Past4WkAnyOtherAddictiveB': category_yes_no,
}

# ,'Staff','SurveyName',

question_list_for_categories = [
  'Program',

  'Staff',
  'SurveyName',

  'AssessmentType',
  'IndigenousStatus',
  'ClientType',
  'CountryOfBirth',
  'LivingArrangement',
  'PDCSubstanceOrGambling',
  'PDCGoals', 
  
  'PDCMethodOfUse',

  'Past4WkUseLedToProblemsWithFamilyFriend',
  'Past4WkHowOftenIllegalActivities',
  'Past4WkHowOftenMentalHealthCausedProblems',
  'Past4WkHowOftenPhysicalHealthCausedProblems',
  'Past4WkDifficultyFindingHousing',
  'HowImportantIsChangeToYou',
  'HowSatisfiedWithProgress',
  'HaveAnySocialSupport',
  'DoYouFeelSafeWhereYouLive'

]

# toplevel_cols = [
# 'PartitionKey',
# # 'Program',
# # 'AssessmentDate',
# # 'AssessmentType',
# 'SurveyData'

# ]

# survey_datacols = [
# 'PartitionKey',
# #'RowKey',
# 'Program',
# #'Staff',
# 'AssessmentDate',
# 'SurveyName',
# 'ClientType',
# 'IndigenousStatus',
# #'PDC',

# 'HaveYouEverInjected'
# ]
# survey_datacols_preregex = [
#    '.*_Score$'
# ]
# survey_datacols_postregex = [
#    '^Past4Wk.*'
# ]

# cols = [
# 'PartitionKey',
# 'RowKey',
# 'Program',
# 'Staff',
# 'AssessmentDate',
# 'SurveyName',
# 'ClientType',
# 'IndigenousStatus',
# 'PDC.PDCSubstanceOrGambling',
# 'PDC.PDCMethodOfUse',
# 'PDC.PDCDaysInLast28',
# 'PDC.PDCUnits',
# 'PDC.PDCHowMuchPerOccasion',

# 'HaveYouEverInjected',
# 'SDSIsAODUseOutOfControl',
# 'SDSDoesMissingFixMakeAnxious',
# 'SDSHowMuchDoYouWorryAboutAODUse',
# 'SDSDoYouWishToStop',
# 'SDSHowDifficultToStopOrGoWithout'
# ]

# rename_cols = {
# 'PDC.PDCSubstanceOrGambling'		: 'PDCSubstanceOrGambling',
# 'PDC.PDCMethodOfUse'		: 'PDCMethodOfUse',
# 'PDC.PDCDaysInLast28'		: 'PDCDaysInLast28',
# 'PDC.PDCUnits'		: 'PDCUnits',
# 'PDC.PDCHowMuchPerOccasion'		: 'PDCHowMuchPerOccasion'
# }


# # categories : ,'Staff','SurveyName',

option_variants = {
    'PDCMethodOfUse': {
      'Ingests': 'Ingest',
      'Injects': 'Inject',
      'Smokes': 'Smoke',
    }
}

data_types = {
  'PartitionKey': 'string',
'RowKey': 'string',
'Program': 'string',
'Staff': 'string',
'AssessmentDate': 'date',
'SurveyName': 'string',


'PDCSubstanceOrGambling': 'string',
'PDCMethodOfUse': 'string',
'PDCDaysInLast28': 'numeric',
'PDCUnits': 'string',
'PDCHowMuchPerOccasion': 'range', # can be 'Other'/NaN/float/int/range -> convert to float64, exclude Other

'HaveYouEverInjected': 'string',
'SDSIsAODUseOutOfControl': 'numeric',
'SDSDoesMissingFixMakeAnxious': 'numeric',
'SDSHowMuchDoYouWorryAboutAODUse': 'numeric',
'SDSDoYouWishToStop': 'numeric',
'SDSHowDifficultToStopOrGoWithout': 'numeric',
}