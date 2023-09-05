import sys
import fire
import pandas as pd
import mylogger
from utils.environment import EnvironmentConfig
from azutil.helper import get_results
# from azutil.az_tables_query import SampleTablesQuery


class FireApp:
    def __init__(self, env:str='local'):
        # if env not in ['local', 'dev', 'prod']:
        #     return "Invalid environment. Choose from 'local', 'dev', or 'prod'."
        
        mylogger.init_logger(env)
        self.logger = mylogger.get(__name__)
        EnvironmentConfig().setup(env)
        self.env = env
        # s = SampleTablesQuery()

        if env == 'local':
            self.logger.info("Running in Local environment.")
            # Your local environment logic here
            
        elif env == 'dev':
            self.logger.info("Running in Development environment.")
            # Your development environment logic here
            
        elif env == 'prod':
            self.logger.info("Running in Production environment.")
            # Your production environment logic here

    def get_table_data(self, start_date:str, end_date:str, table_name:str="ATOM"):
        """
          start_date = 20230701
          end_date = 20230831
        assessment_date_limits = {u"lower": start_date, u"upper": end_date}
        """
        results = get_results(start_date, end_date)
        df = pd.DataFrame.from_records(results)
        self.logger.info(f"df.shape: {df.shape}")


if __name__ == '__main__':
    env = 'local'
    args = sys.argv[1:]

    # Manually parse args to find --env
   # Manually parse sys.argv to find --env
    if '--env' in sys.argv:
        env_index = sys.argv.index('--env')
        try:
            env = sys.argv[env_index + 1]
            # Remove --env and its argument from sys.argv
            del sys.argv[env_index:env_index + 2]
        except IndexError:
            print("ERROR: No value provided for --env")    
    # if '--env' in args:
    #     index = args.index('--env')
    #     env = args[index + 1]  # get the value next to --env
    #     del args[index:index + 2]  # remove --env and its value from args

    app = FireApp(env)
    fire.Fire(app)

# def main(env='local'):
#     app = FireApp(env)
#     fire.Fire(app)

# if __name__ == '__main__':
#     fire.Fire(main)
