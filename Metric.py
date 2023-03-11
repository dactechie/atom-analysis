from dataclasses import dataclass, field
from typing import Callable
import pandas as pd
from statsutil.funcs import remove_outliers, prep


@dataclass
class Metric:
  name: str = ""
  transformations: list[Callable] = field(default_factory=list)
  # remove_outliers: bool = True #set to false if there is a limited_range

  def __post_init__(self):
    self.transformations.append(prep)

  def do_transformations(self, data: pd.DataFrame):
    new_data = data.copy()
    for trans_func in self.transformations:
      new_data = trans_func(new_data)

@dataclass
class RangeBoundedMetric(Metric):
  
  remove_outliers: bool = False

  # def __post_init__(self):
  #   self.transformations.append(prep)

@dataclass
class UnboundedMetric(Metric):
  remove_outliers: bool = True

  def __post_init__(self):
    super().__post_init__()
    self.transformations.append(remove_outliers)
    
# def setup_transformations(metrics:list[Metric]):
#   for metric in metrics:
#     if not isinstance(metric, RangeBoundedMetric):
#       metric.transformations.append(remove_outliers)


# if __name__ == '__main__':
#   pdc_days = RangeBoundedMetric('PDCDaysInLast28')
  
#   metrics:list[Metric] = [pdc_days]
#   # setup_transformations(metrics)

#   for metric in metrics:



# metrics.json
# {
#   'PDCDaysInLast28': {
#       limited_range: 1
#    }
# 
# }
