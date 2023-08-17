# from dataclasses import dataclass
from typing import Union, Optional
import pandas as pd
from pydantic import BaseModel, FieldValidationInfo, field_validator, ValidationError
from data_config import drug_categories, PDC_ODC_fieldsv2
import mylogger

logger = mylogger.get(__name__)

# https://docs.pydantic.dev/dev-v2/migration/
# class Foo(BaseModel):
#     f1: str  # required, cannot be None
#     f2: Optional[str]  # required, can be None - same as str | None
#     f3: Optional[str] = None  # not required, can be None
#     f4: str = 'Foobar'  # not required, but cannot be None


def convert_to_float(v: Union[str, float]) -> float:     
    try:
        return float(v)
    except ValueError:
        raise ValueError(f'Cannot convert {v} to float')    
    # WARNING - Validation error: [{"type":"float_parsing","loc":["used_in_last_4wks"],
    #                               "msg":"Input should be a valid number, unable to parse string as a number",
    #                               "input":"5.4a","url":"https://errors.pydantic.dev/2.1/v/float_parsing"}]    

class DrugUseObject (BaseModel):
  drug_name : str  
  used_in_last_4wks : float
  drug_category: Optional[str] = None

  @field_validator('used_in_last_4wks')
  def float_converter(cls, v:  Union[str, float], info: FieldValidationInfo) -> float:
    return convert_to_float(v)

  def fit_to_drug_category(self) -> 'DrugUseObject':    
    for category, drugs in drug_categories.items():
       if self.drug_name in drugs:
           self.drug_category = category
           return self
    logger.error(f"Drug {self.drug_name} not found in drug_categories")
    return self
    
  
def rename_keys_drug(drug:dict, field_names:dict):
    pdc = drug.copy()
    
    fieldname_drug = field_names['drug_name']
    fieldname_used_in_last4wks = field_names['used_in_last_4wks']

    pdc['drug_name'] = drug[fieldname_drug]
    del pdc[fieldname_drug]

    pdc['used_in_last_4wks'] = drug[fieldname_used_in_last4wks]
    del pdc[fieldname_used_in_last4wks]

    return pdc


if __name__ == '__main__':
    
  data_to_validate = [
          {
            'PDC': [{"PDCSubstanceOrGambling": "Cannabinoids", "PDCDaysInLast28": "20"}],
            'ODC': [{"OtherSubstancesConcernGambling": "Ethanol", "DaysInLast28": 10},
                    {"OtherSubstancesConcernGambling": "Heroin", "DaysInLast28": "5.4"},
                    ],
          },
            {
            'PDC': [{"PDCSubstanceOrGambling": "Cabioids", "PDCDaysInLast28": "0"}],
            'ODC': [{"OtherSubstancesConcernGambling": "Ethanol", "DaysInLast28": 1.23},
                    {"OtherSubstancesConcernGambling": "Heroin", "DaysInLast28": "5.4"},
                    ],
          }          
      
      ]
  
  all_results = []

  try:
     odc_fields = PDC_ODC_fieldsv2['ODC']
     pdc_fields = PDC_ODC_fieldsv2['PDC']

     for index, record in enumerate(data_to_validate):
      logger.debug(f"Parsing record {index}")
      drugs = [rename_keys_drug(odc, odc_fields) for odc in record['ODC']]                    
      pdc = rename_keys_drug(record['PDC'][0], pdc_fields)
      drugs.append(pdc)

      validated_drugs = [DrugUseObject(**drug)  for drug in drugs]
      validated_drugs = [drug.fit_to_drug_category() for drug in validated_drugs]
      all_results.append(validated_drugs)
         
        # d = DrugUseObject(**record, fieldname_drug="PDCSubstanceOrGambling", fieldname_days_in_last28="PDCDaysInLast28") # type: ignore
     
    # d = DrugUseObject(drug_name="Heroin", days_in_last28=10, fieldname_drug="PDCSubstanceOrGambling", fieldname_days_in_last28="PDCDaysInLast28")
    # d1 = DrugUseObject(drug_name="Heroin", days_in_last28=10, fieldname_drug="PDCSubstancOrGambling", fieldname_days_in_last28="PDCDaysInLast28")
  except ValidationError as e:
      logger.warning(f"Validation error: {e.json()}")
      # Check if any of the errors are in the stop_on_error_types
      # if any(isinstance(err.exc, stop_error) for stop_error in CustomConfig.stop_on_error_types for err in e.errors()):
      #     break
  print(all_results)
      
  