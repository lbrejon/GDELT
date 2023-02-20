# import logging
# import os
# from dotenv import find_dotenv, load_dotenv

# # Load environment variables
# load_dotenv(find_dotenv())
# project_dir = os.path.join(os.path.dirname('.env'), os.getcwd())


# # Set log configuration
# LOG_DIR = os.path.join(project_dir, os.environ.get("LOG_DIR"))
# LOGGING_LEVEL = 'INFO'

# # Specify logs display method
# flag_logs_in_file = True
# log_file_path = LOG_DIR + 'upload.log'

# if not flag_logs_in_file:
#     print(f"Logs are displayed into the console")
#     logging.basicConfig(
#         level=LOGGING_LEVEL,
#         format='%(asctime)s :: %(name)s - %(levelname)s :: %(message)s',
#         datefmt='%Y-%m-%d %H:%M:%S',
#     )

# else:
#     print(f"Logs are displayed into log file: {log_file_path}")     
#     logging.basicConfig(
#         filename=log_file_path,
#         encoding='utf-8',
#         # filemode='w',
#         level='INFO',
#         format='%(asctime)s :: %(name)s - %(levelname)s :: %(message)s',
#         datefmt='%Y-%m-%d %H:%M:%S',
#     )


# # # Add filter
# # for handler in logging.root.handlers:
# #         handler.addFilter(logging.Filter('root'))