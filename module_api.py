"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""
# Importing the necessary Libraries
import os
import pandas as pd
from common_utils.db_utils import DB
from common_utils.permission_manager import PermissionManager
import datetime
from time import time
from datetime import datetime
from time import time
import pandas as pd
from io import BytesIO
import base64
import json

# Dictionary to store database configuration settings retrieved from environment variables.
db_config = {
    'host': "amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
    'port': "5432",
    'user':"root",
    'password':"AlgoTeam123"
}          



def get_headers_mapping(module_list,role,user,tenant_id,sub_parent_module,parent_module):
    '''
    Description: The  function retrieves and organizes field mappings, headers, and module features based on the provided module_list, role, user, and other parameters.
    It connects to a database, fetches relevant data, categorizes fields, and compiles features into a structured dictionary for each module.
    '''
    print(module_list,role,user,tenant_id,sub_parent_module,parent_module)
    ##Database connection
    database = DB('AmopAlgouatDB', **db_config)
    # Initialize an empty dictionary to store the output results
    ret_out={}
    # Iterate over each module name in the provided module list
    for module_name in module_list:
        # Fetch field column mappings for the current module and convert the result to a list of dictionaries
        out=database.get_data("field_column_mapping",{"module_name":module_name}).to_dict(orient="records")
        pop_up=[]
        general_fields=[]
        table_fileds={}
        # Categorize the fetched data based on field types
        for data in out:
            if data["pop_col"]:
                pop_up.append(data)
            elif data["table_col"]:
                table_fileds.update({data["db_column_name"]:[data["display_name"],data["table_header_order"]]})
            else:
                general_fields.append(data)    
        # Create a dictionary to store categorized fields
        headers={}
        headers['general_fields']=general_fields
        headers['pop_up']=pop_up 
        headers['header_map']=table_fileds 

        try:
            final_features=[]
            # Fetch all features for the 'super admin' role
            if role.lower()== 'super admin':
                all_features=database.get_data("module_features",{"module":module_name},['features'])['features'].to_list()
                if all_features:
                    final_features=json.loads(all_features[0])
            else:
                # Fetch features based on the role
                role_features=database.get_data("role_module",{"role":role},['module_features'])['module_features'].to_list()
                if role_features:
                    role_features=json.loads(role_features[0])
                # Fetch user-specific features based on the user and tenant ID
                user_features=database.get_data("user_module_tenant_mapping",{"user":user,"tenant_id":tenant_id},['module_features'])['module_features'].to_list()
                if user_features:
                    user_features=json.loads(user_features[0])
                # Determine final features based on the hierarchy of parent module and sub-parent module
                if user_features:
                    print(user_features,'user_features')
                    if not parent_module:
                        final_features=user_features[module_name]
                    elif sub_parent_module:
                        final_features=user_features[parent_module][sub_parent_module][module_name]
                    else:
                        final_features=user_features[parent_module][module_name]

                else:
                    ##fetch the role features
                    print(role_features,'role_features')
                    if not parent_module:
                        final_features=role_features[module_name]
                    elif sub_parent_module:
                        final_features=role_features[parent_module][sub_parent_module][module_name]
                    else:
                        final_features=role_features[parent_module][module_name]

        except Exception as e:
            print(f"there is some error {e}")
            pass
        # Add the final features to the headers dictionary
        headers['module_features']=final_features
        ret_out[module_name]=headers
        
    return ret_out



def form_modules_dict(data,sub_modules,tenant_modules,role_name):
    '''
    Description:The form_modules_dict function constructs a nested dictionary that maps parent modules to their respective submodules and child modules. 
    It filters and organizes modules based on the user's role, tenant permissions, and specified submodules.
    '''
    # Initialize an empty dictionary to store the output
    out={}
    # Iterate through the list of modules in the data
    for item in data:
        parent_module = item['parent_module_name']
        # Skip modules not assigned to the tenant unless the role is 'super admin'
        if (parent_module not in tenant_modules and parent_module) and role_name.lower() != 'super admin':
            continue
        # If there's no parent module, initialize an empty dictionary for the module
        if not parent_module:
            out[item['module_name']]={}
            continue
        else:
            out[item['parent_module_name']]={}
        # Iterate through the data again to find related modules and submodules
        for module in data:
            temp={}
            # Skip modules not in the specified submodules unless the role is 'super admin'
            if (module['module_name'] not in sub_modules and module['submodule_name'] not in sub_modules) and role_name.lower() != 'super admin':
                continue
            # Handle modules without submodules and create a list for them
            if module['parent_module_name'] == parent_module and module['module_name'] and not module['submodule_name']:
                temp={module['module_name']:[]}
            # Handle modules with submodules and map them accordingly
            elif  module['parent_module_name'] == parent_module and module['module_name'] and module['submodule_name']:
                temp={module['submodule_name']:[module['module_name']]}
            # Update the output dictionary with the constructed module mapping
            if temp:
                for key,value in temp.items():
                    if key in out[item['parent_module_name']]:
                        out[item['parent_module_name']][key].append(value[0])
                    elif temp:
                        out[item['parent_module_name']].update(temp)

    # Return the final dictionary containing the module mappings                    
    return out



def transform_structure(input_data):
    '''
    Description:The transform_structure function transforms a nested dictionary of modules into a list of structured dictionaries,
    each with queue_order to maintain the order of parent modules, child modules, and sub-children
    '''
    # Initialize an empty list to store the transformed data
    transformed_data = []
    # Initialize the queue order for parent modules
    queue_order = 1 
    # Iterate over each parent module and its children in the input data
    for parent_module, children in input_data.items():
        transformed_children = []
        child_queue_order = 1
        # Iterate over each child module and its sub-children
        for child_module, sub_children in children.items():
            transformed_sub_children = []
            sub_child_queue_order = 1
            # Iterate over each sub-child module
            for sub_child in sub_children:
                transformed_sub_children.append({
                    "sub_child_module_name": sub_child,
                    "queue_order": sub_child_queue_order,
                    "sub_children": []
                })
                sub_child_queue_order += 1
            # Append the transformed child module with its sub-children
            transformed_children.append({
                "child_module_name": child_module,
                "queue_order": child_queue_order,
                "sub_children": transformed_sub_children
            })
            child_queue_order += 1
        # Append the transformed parent module with its children
        transformed_data.append({
            "parent_module_name": parent_module,
            "queue_order": queue_order,
            "children": transformed_children
        })
        queue_order += 1
    # Return the list of transformed data
    return transformed_data



def get_modules(data):
    '''
    Description:Retrieves and combines module data for a specified user and tenant from the database.
    It executes SQL queries to fetch modules based on user roles and tenant associations, merges the results, removes duplicates, and sorts the data.
    The function then formats the result into JSON, logs audit and error information, and returns the data along with a success or error message.
    '''

    print(f"Request Data: {data}")
    Partner = data.get('Partner', '')
    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
    
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
    
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        print(f"got exception in the restriction")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    ##database connection
    db = DB(database="common_utils", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    try:
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        start_time=0
        date_started=0
        print(f"Exception is {e}")

    # checking the access token valididty
    acess_token = data.get('acess_token', None)
    username = data.get('username', None)
    # if not validate_access_token(acess_token,username):
    #     message = "Invalid Access,Token Cannot Process Request"
    #     return {"flag": False, "message": message}

    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    role_name = data.get('role_name', None)

    # database Connection
    database = DB('AmopAlgouatDB', **db_config)
    try:
        # Retrieving the Modules for the User
        final_modules=[]

        tenant_module_query_params = [tenant_name]
        tenant_module_query = '''SELECT t.id,tm.module_name
                                FROM tenant t JOIN tenant_module tm ON t.id = tm.tenant_id 
                                    WHERE t.tenant_name = %s and tm.is_active = true; '''
        tenant_module_dataframe = database.execute_query(
            tenant_module_query, params=tenant_module_query_params)

        tenant_id = tenant_module_dataframe["id"].to_list()[0]
        main_tenant_modules = tenant_module_dataframe["module_name"].to_list()

        role_module_df = database.get_data("role_module",{"role":role_name},["sub_module"])
        
        role_modules_list = []
        role_main_modules_list=[]
        if not role_module_df.empty:
            role_module = json.loads(role_module_df["sub_module"].to_list()[0])
            for key, value_list in role_module.items():
                role_main_modules_list.append(key)
                role_modules_list.extend(value_list)
            print(role_modules_list,role_main_modules_list,'role_modules_list')

        user_module_df = database.get_data("user_module_tenant_mapping",{"user_name":username,"tenant_id":tenant_id},["module_names"])
            
        user_modules_list = []
        user_main_modules_list=[]
        try:
            if not user_module_df.empty:
                user_module = json.loads(user_module_df["module_names"].to_list()[0])
                for key, value_list in user_module.items():
                    user_main_modules_list.append(key)
                    user_modules_list.extend(value_list)
        except:
            pass
        # Determine the final list of modules based on user and role data
        final_user_role__main_module_list=[]
        if user_modules_list:
            final_user_role__main_module_list=user_main_modules_list
            for item in user_modules_list:
                final_modules.append(item)
        else:
            final_user_role__main_module_list=role_main_modules_list
            for item in role_modules_list:
                final_modules.append(item)
        print(final_user_role__main_module_list,main_tenant_modules)       
        main_tenant_modules = list(set(main_tenant_modules) & set(final_user_role__main_module_list))
        # Retrieve module data and transform it into the required structure
        print(final_modules,main_tenant_modules)
        module_table_df=database.get_data("module",{"is_active":True},["module_name","parent_module_name","submodule_name"],{'id':"asc"}).to_dict(orient="records")
        return_dict=form_modules_dict(module_table_df,final_modules,main_tenant_modules,role_name)
        return_dict=transform_structure(return_dict)
        # Retrieve tenant logo
        logo=database.get_data("tenant",{'tenant_name':tenant_name},['logo'])['logo'].to_list()[0]
        message = "Module data sent sucessfully"
        response = {"flag": True, "message": message, "Modules": return_dict,"logo":logo}
        # End time calculation
        end_time = time()
        time_consumed = end_time - start_time
        try:
            audit_data_user_actions = {
            "service_name": 'Module Management',
            "created_date": date_started,
            "created_by": username,
            "status": str(response['flag']),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": "",
            "module_name": "get_modules","request_received_at":request_received_at
            }
            db.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            print(f"Exception is {e}")
        return response

    except Exception as e:
        print(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting modules"
        # Error Management
        error_data = {"service_name": 'Module_api', "created_date": date_started, "error_messag": message, "error_type": e, "user": username,
                      "session_id": session_id, "tenant_name": tenant_name, "comments": message,
                      "module_name": "", "request_received_at": start_time}
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}

    
    
def get_module_data(data,flag=False,):
    '''
    Retrieves module data for a specified module by checking user and tenant to get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view, handles errors, and logs relevant information.
    '''

    print(f"Request Data: {data}")
    # Extract  fields from Request Data
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    role = data.get('role_name', None)
    module_name=data.get('module_name', None)
    session_id=data.get('session_id', None)
    sub_parent_module=data.get('sub_parent_module', None)
    parent_module=data.get('parent_module', None)
    Partner = data.get('Partner', '')
    # database Connection
    database = DB('AmopAlgouatDB', **db_config)

    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        print(f"got exception in the restriction {e}")
    
    ##database connection for common utils
    db = DB(database="common_utils", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        print("Failed to start ram and time calc")
        pass
    
    try:  
        # Fetch tenant_id based on tenant_name
        tenant_id=database.get_data("tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]
        data_list={}
        pages={}
        if not flag:
            # Create an instance of PermissionManager and call permission manager method
            pm = PermissionManager(db_config)
            # # Retrieving the features for the user
            flag_, features, allowed_sites, allowed_servivceproviders = pm.permission_manager(
                data)
            if not flag_:
                message = "Access Denied"
                return {"flag": False, "message": message}

        # query to find the column mapping for the module
        module_mappings_df = database.get_data('module_column_mappings', {
                                               'module_name': module_name}, ['columns_mapped', 'master_data_flag','tables_mapped','view_name','condition','drop_down_col','main_update_table','order_by','tenant_filter','combine'])
        columns_data = module_mappings_df['columns_mapped'].to_list()[0]
        main_update_table=module_mappings_df['main_update_table'].to_list()[0]
        tenant_filter=module_mappings_df['tenant_filter'].to_list()[0]
        try:
            columns_data=json.loads(columns_data)
        except Exception as e:
            print(f"Exception is {e}")
        master_data_flag = module_mappings_df['master_data_flag'].to_list()[0]
        tables_list = module_mappings_df['tables_mapped'].to_list()[0]
        try:
            tables_list=json.loads(tables_list)
        except Exception as e:
            print(f"Exception is {e}")  
        view_name = module_mappings_df['view_name'].to_list()[0]
        condition = module_mappings_df['condition'].to_list()[0]
        try:
            condition=json.loads(condition)
        except:
            condition={}  
        drop_down_col = module_mappings_df['drop_down_col'].to_list()[0]
        try:
            drop_down_col=json.loads(drop_down_col)
        except Exception as e:
            print(f"Exception is {e}")
        order_by = module_mappings_df['order_by'].to_list()[0]
        try:
            order_by=json.loads(order_by)
        except Exception as e:
            print(f"Exception is {e}")
        combine_all = module_mappings_df['combine'].to_list()[0]
        try:
            combine_all=json.loads(combine_all)
        except Exception as e:
            print(f"Exception is {e}")
        # Check if tables_list is not empty
        if tables_list:
            
            for table in tables_list:
                # Use order_by if current table is main_update_table
                if table == main_update_table:
                    order=order_by
                else:
                    order=None

                if tenant_filter:
                    temp={}
                    temp[tenant_filter]={}
                    temp[tenant_filter]["tenant_id"]=str(tenant_id)
                    temp[tenant_filter].update(condition[tenant_filter])
                    condition[tenant_filter]=temp[tenant_filter]

                if combine_all and table in combine_all:
                    combine=combine_all[table]
                else:
                    combine=[]
                
                if drop_down_col and columns_data and table in drop_down_col and table in columns_data:
                    mod_pages=None
                else:
                    # Get pagination details from data
                    mod_pages = data.get('mod_pages', {})
                    print(mod_pages,'mod_pages')
                # Fetch data based on table, condition, and columns
                if columns_data and table in columns_data and condition:
                    if table in condition:
                        ##fetching the data for the table using conditions
                        data_dataframe=database.get_data(table,condition[table],columns_data[table],order,combine,None,mod_pages)
                    else:
                        ##fetching the data for the table using conditions
                        data_dataframe=database.get_data(table,{},columns_data[table],order,combine,None,mod_pages)
                elif columns_data and table in columns_data:
                    ##fetching the data for the table using conditions
                    data_dataframe=database.get_data(table,{},columns_data[table],order,combine,None,mod_pages)
                elif condition and table in condition and columns_data:
                    if table in columns_data:
                        ##fetching the data for the table using conditions
                        data_dataframe=database.get_data(table,condition[table],columns_data[table],order,combine,None,mod_pages)
                    else:
                        ##fetching the data for the table using conditions
                        data_dataframe=database.get_data(table,condition[table],None,order,combine,None,mod_pages)
                else:
                    ##fetching the data for the table using conditions
                    data_dataframe=database.get_data(table,{},None,order,combine,None,mod_pages)
                # Handle dropdown columns differently
                if drop_down_col and columns_data and table in drop_down_col and table in columns_data:
                    for col in columns_data[table]:
                        #data_list[col]=data_dataframe[col].to_list()
                        data_list[col]=list(set(data_dataframe[col].tolist()))
                else:
                    ##converting the dataframe to dict
                    df=data_dataframe.to_dict(orient='records')
                    if "mod_pages" in data and table == main_update_table:
                        # Calculate pages 
                        pages['start']=data["mod_pages"]["start"]
                        pages['end']=data["mod_pages"]["end"]
                        count_params = [table]
                        count_query = "SELECT COUNT(*) FROM %s" % table
                        count_result = database.execute_query(count_query, count_params).iloc[0, 0]
                        pages['total']=int(count_result)
                    # Add fetched data to data_list
                    data_list[table]=df


        message = "Data fetched successfully"
        if parent_module.lower() == 'people':
            data_list=get_people_data(data_list,module_name,tenant_id)
        
        
            
        #convert all time stamps into str
        new_data = {
                table: (
                    values if isinstance(values[0], str) else [
                        {
                            key: str(value).split('.')[0] if key == 'modified_date' else str(value)
                            for key, value in item.items()
                        }
                        for item in values
                    ]
                )
                for table, values in data_list.items()
            }
        if module_name== 'Feature Codes':
            service_provider_customers=customers_dropdown_data(data,database)
            new_data['service_provider_customers'] = service_provider_customers
            feature_codes = database.get_data("mobility_feature",{}, ["soc_code"])['soc_code'].to_list()
            feature_codes = list(set(feature_codes))
            new_data['feature_codes'] = feature_codes
        if module_name== 'Optimization Group':
            rate_plans_list=rate_plan_dropdown_data(data,database)
            new_data['rate_plans_list'] = rate_plans_list
        if module_name == 'Customer Rate Plan':
            # List of keys that need rounding
            keys_to_round = {"surcharge_3g", "plan_mb", "base_rate", "rate_charge_amt"}

            # Process the dictionary
            for table, records in new_data.items():
                if isinstance(records, list):  # Ensure records are a list of dictionaries or strings
                    for record in records:
                        if isinstance(record, dict):  # Ensure each record is a dictionary
                            for key, value in record.items():
                                if key in keys_to_round:
                                    if isinstance(value, (int, float)):
                                        record[key] = round(value, 2)
                                    elif isinstance(value, str):
                                        try:
                                            # Try converting the string to a float and rounding
                                            record[key] = round(float(value), 2)
                                        except ValueError:
                                            # If conversion fails, leave the value as is
                                            print(f"Cannot round non-numeric string: {value}")
                        elif isinstance(record, str):
                            print(f"String record found: {record}")
                        else:
                            print(f"Unexpected record format: {record}")
                else:
                    print(f"Unexpected records format: {records}")
        
        # Response including pagination metadata
        if not flag:
            #calling get header to get headers mapping   
            headers_map=get_headers_mapping([module_name],role,username,tenant_id,sub_parent_module,parent_module) 
            response = {"flag": True,"message":message, "data": new_data, "pages":pages,"features": features ,"headers_map":headers_map}
            # End time calculation
            end_time = time()
            time_consumed = end_time - start_time
            try:
                audit_data_user_actions = {"service_name": 'Module Management',"created_date": date_started,
                "created_by": username,
                    "status": str(response['flag']),
                    "time_consumed_secs": time_consumed,
                    "session_id": session_id,
                    "tenant_name": Partner,
                    "comments": message,
                    "module_name": "get_module_data","request_received_at":request_received_at
                }
                db.update_audit(audit_data_user_actions, 'audit_user_actions')
            except Exception as e:
                print(f"Exception is {e}")
            return response
        else:
            return new_data,pages


    except Exception as e:
        print(F"Something went wrong and error is {e}")
        message = "Something went wrong fetching module data"
        response={"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'get_module_data',"created_date": date_started,"error_message": message,"error_type": error_type,"users": username,"session_id": session_id,"tenant_name": Partner,"comments": "","module_name": "Module Managament","request_received_at":request_received_at}
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            print(f"Exception is {e}")
        if not flag:
            return response
        else:
            return []
        
def customers_dropdown_data(data,database):
    # database = DB('AmopAlgouatDB', **db_config)
    # List of service provider names
    serviceproviders = database.get_data("serviceprovider",{"is_active":True}, ["service_provider_name"])['service_provider_name'].to_list()
    service_provider_names = list(set(serviceproviders))
    # Initialize an empty dictionary to store the results
    service_provider_customers = {}
    
    # Iterate over each service provider name
    for service_provider_name in service_provider_names:
        # Get the tenant_id for the current service provider name
        tenant_id = database.get_data(
            "serviceprovidertenantconfiguration", 
            {'service_provider_name': service_provider_name}, 
            ["tenant_id"]
        )['tenant_id'].to_list()[0]

        # Get the customer names associated with the tenant_id
        customer_names = database.get_data(
            "customers", 
            {'tenant_id': str(tenant_id)}, 
            ["customer_name"]
        )['customer_name'].to_list()

        # Add the result to the dictionary
        service_provider_customers[service_provider_name] = customer_names

    # Print the resulting dictionary
    return service_provider_customers
    

def rate_plan_dropdown_data(data,database):
    # Database connection
    #database = DB('AmopAlgouatDB', **db_config)
    
    # Get unique service provider names
    serviceproviders = database.get_data("serviceprovider",{"is_active":True}, ["service_provider_name"])['service_provider_name'].to_list()
    service_provider_names = list(set(serviceproviders))
    
    # Initialize an empty dictionary to store the results
    rate_plans_list = {}
    
    # Iterate over each service provider name
    for service_provider_name in service_provider_names:
        # Get the rate plan codes for the current service provider name
        rate_plan_items = database.get_data(
            "carrier_rate_plan", 
            {'service_provider': service_provider_name,"is_active":True}, 
            ["rate_plan_code"]
        )['rate_plan_code'].to_list()
        
        # Add the result to the dictionary
        rate_plans_list[service_provider_name] = rate_plan_items
    
    # Return the resulting dictionary
    return rate_plans_list



def form_Partner_module_access(tenant_id,module_data):
    '''
    Description:Forms access information for a specific tenant by filtering and organizing modules.
    The function processes tenant-specific module data and structures it for use in the system.
    '''
    # Extract the 'tenant_module' list from module_data
    tenant_module=module_data['tenant_module']
    # Initialize an empty list to hold the module names for the specified tenant
    modules_names=[]
    # Iterate over each item in tenant_module to filter modules for the specified tenant_id
    for data_item in tenant_module:
        if str(data_item['tenant_id']) == str(tenant_id):
            print(data_item['tenant_id'],tenant_id)
            modules_names.append(data_item['module_name'])
    # Extract the 'module' list from module_data  
    modules_data=module_data['module']
     # Initialize an empty list to hold unique parent module names and a dictionary to structure modules by parent
    return_modules=[]
    modules={}
    print(modules_names,"module ids")
    # Iterate over each item in modules_data to organize modules by their parent module name
    for data_item in modules_data:
        # Add parent module name to return_modules if it's not already present
        if not data_item['parent_module_name'] or data_item['parent_module_name'] == "None":
            continue
        # Initialize an empty list for the parent module in the modules dictionary if it's not already there
        if data_item['parent_module_name'] not in return_modules:
            return_modules.append(data_item['parent_module_name'])
        if data_item['parent_module_name'] not in modules:
            modules[data_item['parent_module_name']]=[]
        modules[data_item['parent_module_name']].append(data_item['module_name'])
    # Update the original module_data with the filtered and organized modules
    module_data["tenant_module"]=return_modules
    module_data['module']=modules
     # Return the modified module_data with tenant-specific access information
    return module_data



def get_user_module_map(data):
    '''
    Description:Retrieves the module mapping for a user based on their role and tenant information.
    The function checks for existing user-module mappings and falls back to role-based mappings if necessary.
    '''
    print(f"Request Data: {data}")
    try:
        # Extract username, role, and tenant_name from the input data dictionary
        username = data.get('username', None)
        role = data.get('role', None)
        tenant_name = data.get('tenant_name', None)
        # Initialize the database connection
        database = DB('AmopAlgouatDB', **db_config)
        return_dict={}
        # Retrieve tenant_id based on tenant_name
        tenant_id=database.get_data("tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]
        flag=False
        # Fetch the user-module mapping from the database
        user_map=database.get_data("user_module_tenant_mapping",{"user_name":username,"tenant_id":tenant_id},["module_names","sub_module","module_features"]).to_dict(orient="records")
        # Check if a user-specific module map exists
        if user_map:
            user_map=user_map[0]
            for key,value in user_map.items():
                if value:
                    flag=True
                    break
            if flag:
                # Rename 'module_names' to 'module'
                return_dict=user_map
                return_dict['module']=user_map['module_names']
                return_dict.pop('module_names')
            else:
                # If the user map is empty, fall back to role-based mapping
                return_dict=database.get_data("role_module",{"role":role},["module","sub_module","module_features"]).to_dict(orient="records")
        else:
            # If no user-specific map is found, retrieve the role-based module map
            return_dict=database.get_data("role_module",{"role":role},["module","sub_module","module_features"]).to_dict(orient="records")
         # Return the final mapping data with a success flag
        return {'flag':True,'data':return_dict}

    except Exception as e:
        # Handle any exceptions that occur during data retrieval
        print(f"error in fetching data {e}")
        response={'Flag':False,'data':{}}
        return response



def get_partner_info(data):
    print(f"Request Data: {data}")
    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
    
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
    
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        print(f"got exception in the restriction")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    user_name = data.get('user_name', '')
    Partner = data.get('Partner', '')
    ##database connection
    db = DB(database="common_utils", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        print("Failed to start ram and time calc")
        pass

    # checking the access token valididty
    acess_token = data.get('acess_token', None)
    username = data.get('username', None)
    # if not validate_access_token(acess_token,username):
    #     message = "Invalid Access,Token Cannot Process Request"
    #     return {"flag": False, "message": message}

    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    role = data.get('role', None)
    modules_list = data.get('modules_list', None)
    parent_module = data.get('parent_module', None)
    sub_parent_module = data.get('sub_parent_module', None)
    # database Connection
    database = DB('AmopAlgouatDB', **db_config)
    
    try:
        counts={}
        returning_partner_module_data={}

        
        partner=tenant_name
        sub_partner=''
        tenant_df=database.get_data("tenant",{'tenant_name':tenant_name},['parent_tenant_id','id','email_ids','logo'])
        parent_tenant_id=tenant_df['parent_tenant_id'].to_list()[0]
        tenant_id=tenant_df['id'].to_list()[0]
        email_id=tenant_df['email_ids'].to_list()[0]
        logo=tenant_df['logo'].to_list()[0]
        if parent_tenant_id:
            sub_partner=tenant_name
            partner=database.get_data("tenant",{'id':parent_tenant_id},['tenant_name'])['tenant_name'].to_list()[0]

        #formating data for partner info
        if 'Partner info' in modules_list:
            try:
                email_id=json.loads(email_id)
            except Exception as e:
                print(f"Exception is {e}")
            returning_partner_module_data['Partner info']={'partner':partner,'sub_partner':sub_partner,'email_id':email_id,'logo':logo}
            modules_list.remove("Partner info")
            print(returning_partner_module_data)

        #formating data for athentication
        if 'Partner authentication' in modules_list:
            returning_partner_module_data['Partner authentication']={}
            modules_list.remove("Partner authentication")
            print(returning_partner_module_data)

        for module in modules_list:
            data["module_name"]=module
            if module in data["pages"]: 
                data["mod_pages"]=data["pages"][module]
            module_data,pages=get_module_data(data,True)
            if pages:
                counts[module]={"start":pages["start"],"end":pages["end"],"total":pages["total"]}

            #formating data for Partner module Access 
            if module=="Partner module access":
                module_data=form_Partner_module_access(tenant_id,module_data)
                filtered_roles = [role for role in module_data["role_name"] if role != "Super Admin"]
                module_data["role_name"] = list(set(filtered_roles))

            #formating data for Customer groups
            if module=="Customer groups":
                module_data_new={}
                module_data_new['customer_names']=[]
                module_data_new['billing_account_number']=[]
                # Loop through each dictionary in the list
                for dic in module_data["customers"]:
                    # Create new dictionaries for each key-value pair
                    first_key, first_value = list(dic.items())[0]
                    second_key, second_value = list(dic.items())[1]
                    if first_key =='billing_account_number':
                        if first_value and first_value != "None":
                            module_data_new['billing_account_number'].append(first_value)
                        if second_value and second_value != "None":
                            module_data_new['customer_names'].append(second_value)
                    else:
                        if first_value and first_value != "None":
                            module_data_new['customer_names'].append(first_value)
                        if second_value and second_value != "None":
                            module_data_new['billing_account_number'].append(second_value)
                module_data.update(module_data_new)
                module_data.pop("customers")  

            #formating data for Partner users
            if module=="Partner users":
                result_dict = {}
                
                module_data['role_name']=list(set(module_data['role_name']))
                for d in module_data['tenant']:
                    tenant_id = d["id"]
                    tenant_name = d["tenant_name"]
                    parent_id = d["parent_tenant_id"]
                    sub_temp=[]
                    if parent_id == "None":
                        for di in module_data['tenant']:
                            print(di,tenant_id)
                            if di["parent_tenant_id"] != "None" and float(di["parent_tenant_id"])==float(tenant_id):
                                sub_temp.append(di["tenant_name"])
                        result_dict[tenant_name]=sub_temp
                module_data['tenant']=result_dict
                user_df=database.get_data("users",{},['is_active','migrated']).to_dict(orient='records')
                total_count=len(user_df)
                active_user_count=0
                migrated_count=0
                for user in user_df:
                    if user['is_active']==True:
                        active_user_count=active_user_count+1
                    if user['migrated']==True:
                        migrated_count=migrated_count+1
                module_data['total_count']=total_count
                module_data['active_user_count']=active_user_count
                module_data['migrated_count']=migrated_count

            #final addition of modules data into the retuting dict
            returning_partner_module_data[module]=module_data
            returning_partner_module_data["pages"]=counts

        #calling get header to get headers mapping    

        headers_map=get_headers_mapping(modules_list,role,username,tenant_id,sub_parent_module,parent_module)
        message=f"partner data fetched successfully"
        response = {"flag": True, "data": returning_partner_module_data,"headers_map":headers_map,"message":message}
        # End time calculation
        end_time = time()
        time_consumed = end_time - start_time
        try:
            audit_data_user_actions = {"service_name": 'Module Management',"created_date": date_started,
            "created_by": username,
                "status": str(response['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "get_partner_info","request_received_at":request_received_at
            }
            db.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            print(f"Exception is {e}")
        return response
        
    except Exception as e:
        print(F"Something went wrong and error is {e}")
        message = "Something went wrong while fetching Partner info"
        response={"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'Module Management',"created_date": date_started,"error_message": message,"error_type": error_type,"users": username,"session_id": session_id,"tenant_name": Partner,"comments": "","module_name": "get_partner_info","request_received_at":request_received_at}
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            print(f"Exception is {e}")
        return response
        
    
def create_module_access_info(data):
    """
    Processes role-based access data to generate a dictionary with module, submodule, and feature information.

    Args:
        data (dict): Dictionary containing roles as keys and their corresponding modules and features as values.

    Returns:
        dict: A dictionary with roles as keys and JSON-encoded strings of modules, submodules, and features as values.
    """
    return_dict={}
    modules_list = []
    submodules_list = {}
    features_dict = {}
    # Iterate over each role and its associated features
    for role,user_features in data.items():
        print(user_features)
        for main_module, content in user_features.items():
            modules_list.append(main_module)
            submodules_list[main_module]=content['Module']
            # Initialize the features dictionary for the current main module
            features_dict[main_module]={}
            features=content['Feature']
            # Iterate over each submodule of the main module
            for submodule in content['Module']:
                if submodule in features:
                    features_dict[main_module][submodule]=features[submodule]
         # Store the processed data for the current role
        return_dict[role]={"module":json.dumps(modules_list),"sub_module":json.dumps(submodules_list),"module_features":json.dumps(features_dict)}
     # Return the final dictionary with role-based access information
    return return_dict



def update_partner_info(data):
    """
    Updates partner information in the database based on provided data and performs various operations 
    such as validation, updating, or inserting records in different modules.

    Args:
        data (dict): Dictionary containing details such as Partner, request timestamps, session IDs, 
                     access tokens, and other data needed for updating or inserting records.

    Returns:
        dict: Response indicating the success or failure of the operation with a message.
    """
    print(f"Request Data: {data}")
    Partner = data.get('Partner', '')
    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
    
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
    
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        print(f"got exception in the restriction")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    user_name = data.get('user_name', '')
    ##database connection
    db = DB(database="common_utils", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        print("Failed to start ram and time calc")
        pass
    # Extracting necessary fields from the data 
    acess_token = data.get('acess_token', None)
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    updated_data_ui = data.get('changed_data', [])
    session_id = data.get('session_id', None)
    where_dict=data.get('where_dict', {})
    role = data.get('role_name', None)
    action = data.get('action', None)
    module_name = data.get('module_name', None)
    module_name=module_name.lower()
    # database Connection
    database = DB('AmopAlgouatDB', **db_config)
    
    try:
        # Fetch tenant ID based on tenant name
        tenant_id=database.get_data("tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]
        # Prepare updated data for database operations
        updated_data={}
        for key,value in updated_data_ui.items():
            if type(value) != str:
                updated_data[key]=json.dumps(value)
            elif value == 'None' or value == '':
                updated_data[key]=None
            else:
                updated_data[key]=value
        # Remove 'id' field if present and module_name is not 'customer groups'  
        if "id" in updated_data and module_name !='customer groups':
            updated_data.pop("id")
        # Perform database operations based on the module_name and action
        if module_name=='partner info':
            where_dict={"tenant_name":tenant_name}
            if action == "update" or action == "delete":
                database.update_dict("tenant",updated_data,where_dict)
            elif action == "create":
                database.insert_dict(updated_data,"tenant")

        if module_name=="partner module access":
            if action == "update" or action == "delete":
                role_module_acess=create_module_access_info(updated_data_ui)
                for role,value_dict in role_module_acess.items():
                    where_dict={"role":role}
                    database.update_dict("role_module",value_dict,where_dict)
            elif action == "create":
                role_module_acess=create_module_access_info(updated_data_ui)
                for role,value_dict in role_module_acess.items():
                    value_dict['role']=role
                    database.insert_dict(value_dict,"role_module")
    
        if module_name=="customer groups":
            if action == "update" or action == "delete":
                where_dict={"id":updated_data['id']}
                database.update_dict("customergroups",updated_data,where_dict)
            elif action == "create":
                if 'id' in updated_data:
                    updated_data.pop('id')
                if 'modified_date' in updated_data:
                    updated_data.pop('modified_date')
                database.insert_dict(updated_data,"customergroups")
                
        if module_name=="partner users":
            if "tenant_id" in updated_data and updated_data['tenant_id']:
                updated_data['tenant_id']=str(int(float(updated_data["tenant_id"])))
                
            if action == "delete":
                database.update_dict("users",updated_data,{"username":updated_data["username"]})

            elif action == "update":
                
                customer_info=updated_data.get('customer_info')
                try:
                    customer_info=json.loads(customer_info)
                except Exception as e:
                    print(f"Exception is {e}")
                if customer_info:

                    db_customer_info={}
                    for key,value in customer_info.items():
                        if type(value) != str:
                            db_customer_info[key]=json.dumps(value)
                        elif value == 'None':
                            db_customer_info[key]=None
                        else:
                            db_customer_info[key]=value

                    database.update_dict("users",db_customer_info,{"username":customer_info["username"]})

                user_update=updated_data.get('user_info')
                try:
                    user_update=json.loads(user_update)
                except Exception as e:
                    print(f"Exception is {e}")
                if user_update:

                    db_user_update={}
                    for key,value in user_update.items():
                        if type(value) != str:
                            db_user_update[key]=json.dumps(value)
                        elif value == 'None':
                            db_user_update[key]=None
                        else:
                            db_user_update[key]=value

                    database.update_dict("users",db_user_update,{"username":db_user_update["username"]})

                user_modules_update=updated_data.get('data')
                try:
                    user_modules_update=json.loads(user_modules_update)
                except Exception as e:
                    print(f"Exception is {e}")
                if user_modules_update:
                    tenant_id=database.get_data("tenant",{'tenant_name':updated_data["Selected Partner"]},['id'])['id'].to_list()[0]
                    user_module_acess=create_module_access_info(user_modules_update)
                    print(user_module_acess,updated_data)
                    for user,value_dict in user_module_acess.items():
                        value_dict['module_names']=value_dict['module']
                        value_dict.pop('module')
                        database.update_dict("user_module_tenant_mapping",value_dict,{"tenant_id":tenant_id,"user_name":updated_data["Username"]})

            elif action == "create":

                user_update=updated_data.get('user_info')
                try:
                    user_update=json.loads(user_update)
                except Exception as e:
                    print(f"Exception is {e}")

                tenants_list=[]
                tenants_list.append(user_update["tenant_name"])
                tenants_list.extend(user_update["subtenant_name"])

                for tenant_name in tenants_list:
                    tenant_id=database.get_data("tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]
                    db_user={"tenant_id":tenant_id,"user_name":user_update["username"]}
                    database.insert_dict(db_user,"user_module_tenant_mapping")

                if user_update:

                    db_user_update={}
                    for key,value in user_update.items():
                        if type(value) != str:
                            db_user_update[key]=json.dumps(value)
                        elif value == 'None':
                            db_user_update[key]=None
                        else:
                            db_user_update[key]=value

                    database.insert_dict(db_user_update,"users")


        message='Updated sucessfully'
        response = {"flag": True, "message": message}
        # End time calculation
        end_time = time()
        time_consumed = end_time - start_time
        try:
            audit_data_user_actions = {"service_name": 'Module Management',"created_date": date_started,
            "created_by": username,
                "status": str(response['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": json.dumps(updated_data_ui),
                "module_name": "update_partner_info","request_received_at":request_received_at
            }
            db.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            print(f"Exception is {e}")
        return response
        
    except Exception as e:
        print(F"Something went wrong {e}")
        message = "Something went wrong while updating Partner info"
        response={"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'Module Management',"created_date": date_started,"error_message": message,"error_type": error_type,"users": username,"session_id": session_id,"tenant_name": Partner,"comments": message,"module_name": "update_partner_info","request_received_at":request_received_at}
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            print(f"Exception is {e}")
        return response




def get_people_data(data_list,module_name,tenant_id):

    database = DB('AmopAlgouatDB', **db_config)
    print(tenant_id,"tenant_id")

    if module_name=='Bandwidth Customers':
        band_id=[]
        
        for data_item in data_list['customers']:
            if data_item['bandwidth_customer_id']:
                band_id.append(data_item['bandwidth_customer_id'])
                
        if band_id:
            query=f"SELECT  bws.id as bandwidth_unique_col,bws.bandwidth_account_id,bws.bandwidth_customer_name,c.bandwidth_customer_id FROM customers as c join bandwidth_customers as bws on bws.id = CAST(c.bandwidth_customer_id AS INTEGER)  WHERE bws.id in {tuple(band_id)} ORDER BY c.modified_date desc;"
            df=database.execute_query(query,True)
            bandwidth_account_id=df['bandwidth_account_id'].to_list()[0]
            data_out=df.to_dict(orient='records')
            
            global_account_number_df=database.get_data('bandwidthaccount',{'id':bandwidth_account_id},["id","global_account_number"])
            global_account_number=global_account_number_df['global_account_number'].to_list()[0]
            bandwidthaccount_unique_col=global_account_number_df['id'].to_list()[0]
            merged_list=[]
            dict2 = {d['bandwidth_customer_id']: d for d in data_list['customers']}
            for dic_data_out in data_out:
                # Find the corresponding dictionary in dict2
                dic_data_list = dict2.get(dic_data_out['bandwidth_customer_id'], {})
                # Merge dictionaries
                merged_dict = {**dic_data_list, **dic_data_out}
                merged_dict.update({"global_account_number":global_account_number})
                merged_dict.update({"bandwidthaccount_unique_col":bandwidthaccount_unique_col})
                merged_list.append(merged_dict)

            data_list['customers']=merged_list
            
        return data_list
        

    elif module_name=='E911 Customers':
        e911_customer_ids=tuple(data_list['e911_customer_id'])
        if e911_customer_ids:
            df=database.get_data("e911customers", {'id':e911_customer_ids},order={"modified_date":"desc"})
            data_list['e911_customers']=df.to_dict(orient='records')
            data_list.pop('e911_customer_id')
        return data_list
    
    elif module_name=='NetSapiens Customers':
        reseller_ids=database.get_data("customers",{'netsapiens_reseller_id':"not Null"},['id']).to_list()
        
        for data_item in data_list['customers']:
            if data_item['id'] in reseller_ids:
                data_item['netsapiens_type']='Reseller'
            else:
                data_item['netsapiens_type']='Domain'
                
        return data_list 
    


def update_people_data(data):
    '''
    updates module data for a specified module by checking user and tenant to get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view, handles errors, and logs relevant information.
    '''
    Partner = data.get('Partner', '')
    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
    
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
    
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        print(f"got exception in the restriction")
    
    print(f"Request Data: {data}")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    user_name = data.get('user_name', '')
    ##database connection
    dbs = DB(database="common_utils", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        print("Failed to start ram and time calc")
        pass
    try:
        action = data.get('action','')
  
        ui_changed_data = data.get('changed_data', {})
        module_name = data.get('module', {})
        # Database Connection (Ensure you have a proper DB connection setup)
        database = DB('AmopAlgouatDB', **db_config)

        df=database.get_data('module_column_mappings',{"module_name":module_name},["main_update_table","sub_table_mapping","unique_columns"])
        main_update_table=df['main_update_table'].to_list()[0]
        try:
            sun_table_map=json.loads(df['sub_table_mapping'].to_list()[0])
        except:
            sun_table_map={}
        print()
        try:
            unique_column=json.loads(df['unique_columns'].to_list()[0])
        except:
            unique_column=df['unique_columns'].to_list()

        changed_data={}
        for key,value in ui_changed_data.items():
            if value and value != None and value != "None":
                changed_data[key]=value
            else:
                changed_data[key]=None
        
        update_dics={}
        for table,cols in sun_table_map.items():
            if table not in update_dics:
                update_dics[table]={}
            for col in cols:
                if col in changed_data and changed_data[col]:
                    update_dics[table][col]=changed_data[col]
                if col in changed_data:
                    changed_data.pop(col)
                    
        db = DB(database="AmopAlgouatDB", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
        if action=='update' or action  == 'delete':
            
            print(changed_data,update_dics,unique_column)
            unique_column_val=changed_data.pop(unique_column[main_update_table])
            db.update_dict(main_update_table,changed_data ,{unique_column[main_update_table]:unique_column_val})
            
            for table,update_cols in update_dics.items():
                if table not in unique_column:
                    continue
                subunq_val=update_cols[unique_column[table]]
                update_cols.pop(unique_column[table])
                db.update_dict(table,update_cols ,{'id':subunq_val})
        else:
            if 'id' in changed_data:
                changed_data.pop("id")
            if 'bandwidth_customer_id' in changed_data:
                changed_data.pop("bandwidth_customer_id")
            if 'netsapiens_customer_id' in changed_data:
                changed_data.pop("netsapiens_customer_id")
            db.insert_dict(changed_data, main_update_table)
            
        message = f"Updated sucessfully"
        response = {"flag": True, "message": message}
        # End time calculation
        end_time = time()
        time_consumed = end_time - start_time
        try:
            audit_data_user_actions = {"service_name": 'Module Management',"created_date": date_started,
            "created_by": user_name,
                "status": str(response['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": json.dumps(changed_data),
                "module_name": "update_people_data","request_received_at":request_received_at
            }
            dbs.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            print(f"Exception is {e}")
        
        return response
                    
    except Exception as e:
        print(f"An error occurred: {e}")
        message = f"Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'update_people_data',"created_date": date_started,"error_message": message,"error_type": error_type,"users": user_name,"session_id": session_id,"tenant_name": Partner,"comments":message,"module_name": "Module Managament","request_received_at":request_received_at}
            dbs.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            print(f"Exception is {e}")
        return response
  


def get_superadmin_info(data):
    '''
    Description:The get_superadmin_info function retrieves and formats superadmin-related data based on user permissions and requested modules, 
    handling various sub-modules and tabs. It performs permission checks, database queries, and error logging, while measuring performance and updating audit records.
    '''
    print(f"Request Data: {data}")
    Partner = data.get('Partner', '')

    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
    
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
    
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        print(f"got exception in the restriction")
    ##Retrieving the data
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    ##database connection
    db = DB(database="common_utils", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        print("Failed to start ram and time calc")
        pass

    # checking the access token valididty
    access_token = data.get('access_token', None)
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    sub_parent_module=data.get('sub_parent_module', None)
    parent_module=data.get('parent_module', None)
    # database Connection
    database = DB('AmopAlgouatDB', **db_config)
    try:
        role_name = data.get('role_name', None)
        data_dict_all = {}
        # Retrieve the main tenant ID based on the tenant name
        main_tenant_id=database.get_data("tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]
         # Check if the user is a superadmin
        if role_name and role_name.lower() != 'super admin':
            message = 'Module is not enabled for this user since he is not a superadmin'
            response_data = {"flag": True, "message": message}
            print(response_data)
        else:
            sub_module = data.get('sub_module', None)
            sub_tab = data.get('sub_tab', '')
            # Handle different sub-modules and tabs  
            if sub_module and sub_module.lower() == 'partner api':  
                Environment = data.get('Environment', '')
                Partner = data.get('Selected_Partner', '')
                environment_list = ['SandBox', 'QA', 'UAT', 'Prod']
                partner_list = get_tenant_list(database)

                if sub_tab.lower() == 'carrier apis':
                    # Fetch carrier API data
                    carrier_api_data = get_data_and_format(database, "carrier_apis", Environment, Partner)
                    data_dict_all = {
                        "Carrier_apis_data": carrier_api_data,
                        "Environment": environment_list,
                        "Partner": partner_list
                    }
                    # Convert timestamps to a consistent format
                    data_dict_all = convert_timestamps(data_dict_all) 
                    headers_map=get_headers_mapping(["carrier apis"],role_name,username,main_tenant_id,sub_parent_module,parent_module)
                elif sub_tab.lower() == 'amop apis':
                    # Fetch Amop API data
                    amop_api_data = get_data_and_format(database, "amop_apis", Environment, Partner)
                    data_dict_all = {
                        "amop_apis_data": amop_api_data,
                        "Environment": environment_list,
                        "Partner": partner_list
                    }
                    # Convert timestamps to a consistent format
                    data_dict_all = convert_timestamps(data_dict_all)
                    headers_map=get_headers_mapping(["amop apis"],role_name,username,main_tenant_id,sub_parent_module,parent_module)
            elif sub_module and sub_module.lower() == 'partner modules':
                # Extract 'sub_partner' and 'Partner' from the data dictionary
                flag = data.get('flag', '')
                ##tenants and sub tenants
                parent_tenant_df = database.get_data('tenant',{'parent_tenant_id':"Null"},["id","tenant_name"])
                tenant_dict = {}

                # Step 2: Fetch all tenants and iterate over parent tenants to build the dictionary
                all_tenants_df = database.get_data('tenant',None,["tenant_name","parent_tenant_id"])

                # Step 3: Build the dictionary with parent-child relationships
                for _, parent_row in parent_tenant_df.iterrows():
                    parent_tenant_name = parent_row['tenant_name']
                    parent_tenant_id = parent_row['id']
                    # Filter the child tenants from the complete tenants DataFrame
                    child_tenants = sorted(all_tenants_df[all_tenants_df['parent_tenant_id'] == parent_tenant_id]['tenant_name'].tolist())
                    tenant_dict = dict(sorted(tenant_dict.items()))
                    # Add to the dictionary
                    tenant_dict[parent_tenant_name] = child_tenants
                if flag=='withoutparameters':
                    data_dict_all = {
                            "partners_and_sub_partners": tenant_dict

                        }
                else:
                    sub_partner = data.get('sub_partner', '')
                    partner = data.get('Selected_Partner', '')
                    # Determine the value to use for the query
                    query_value = sub_partner if sub_partner else partner

                    if query_value:
                        try:
                            tenant_query_dataframe = database.get_data("tenant",{"tenant_name":query_value},["id"])
                            tenant_id = tenant_query_dataframe.iloc[0]['id']
                        except Exception as e:
                            # Log the exception for debugging
                            print(f"An error occurred while executing the query: {e}")
                    else:
                        print("No valid value provided for the query.")
                    ##roles dataframe for the tenant
                    role_dataframe = database.get_data('roles',{"tenant_id":int(tenant_id)},['id','role_name','is_active','created_by','modified_by','modified_date'],{"modified_date":"desc"})
                    roles_data = role_dataframe.to_dict(orient='records')  # Convert to dictionary of lists
                    ##tenant_module
                    role_module_dataframe = database.get_data('tenant_module',{"tenant_id":int(tenant_id)},['id','module_name','is_active','modified_by','modified_date'],{"modified_date":"desc"})
                    role_module_data = role_module_dataframe.to_dict(orient='records')  # Convert to dictionary of lists
                    
                    data_dict_all = {
                            "roles_data": roles_data,
                            "role_module_data":role_module_data,
                            "partners and sub partners": tenant_dict

                        }
                data_dict_all = convert_timestamps(data_dict_all)
                headers_map=get_headers_mapping(["partner module","role partner module"],role_name,username,main_tenant_id,'',parent_module)

    
        response = {"flag": True, "data": data_dict_all ,"headers_map":headers_map}
        # End time calculation
        end_time = time()
        time_consumed = end_time - start_time
        try:
            audit_data_user_actions = {"service_name": 'Module Management',"created_date": date_started,
            "created_by": username,
                "status": str(response['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": "",
                "module_name": "get_superadmin_info","request_received_at":request_received_at
            }
            db.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            print(f"Exception is {e}")
        return response
        
    except Exception as e:
        print(F"Something went wrong and error is {e}")
        message = "Something went wrong getting Super admin info"
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'get_superadmin_info',"created_date": date_started,"error_message": message,"error_type": error_type,"users": username,"session_id": session_id,"tenant_name": Partner,"comments": "","module_name": "Module Managament","request_received_at":request_received_at}
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            print(f"Exception is {e}")
        return {"flag": False, "message": message}




def get_data_and_format(database, table_name, Environment=None, Partner=None):
    """
    Fetches data from a specified database table and formats it into a dictionary.

    Parameters:
    - database (DB): The database object used to execute the query.
    - table_name (str): The name of the table to fetch data from.
    - Environment (str, optional): An optional filter for the environment.
    - Partner (str, optional): An optional filter for the partner.

    Returns:
    - dict: A dictionary containing the table name as the key and a list of records as the value.
    """

    if Environment and Partner:
        # Fetch data from the table with filters for Environment and Partner
        dataframe = database.get_data(table_name, {"env": Environment, "partner": Partner},None,{"last_modified_date_time":"desc"})
    else:
        # Fetch data from the table without filters
        dataframe = database.get_data(table_name,None,None,{"last_modified_date_time":"desc"})
    
    # Convert the dataframe to a list of dictionaries
    data_list = dataframe.to_dict(orient='records')
    
    # Structure the data in the desired format
    formatted_data = {table_name: data_list}
    
    return formatted_data


def get_tenant_list(database, include_sub_tenants=False):
    if include_sub_tenants:
        tenant_query_dataframe = database.get_data("tenant",{"parent_tenant_id":"not Null"},["tenant_name"])
    else:
        tenant_query_dataframe = database.get_data("tenant",{"parent_tenant_id":"Null"},["tenant_name"])
    
    return tenant_query_dataframe['tenant_name'].to_list()


# Function to convert Timestamps to strings
def convert_timestamps(obj):
    if isinstance(obj, dict):
        return {k: convert_timestamps(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_timestamps(elem) for elem in obj]
    elif isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    return obj



def update_superadmin_data(data):
    
    '''
    updates module data for a specified module by checking user and tenant to get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view, handles errors, and logs relevant information.
    '''

    print(f"Request Data: {data}")
    Partner = data.get('Partner', '')
    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
    
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
    
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        print(f"got exception in the restriction")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', '')
    module_name = data.get('module_name', '')
    changed_data = data.get('changed_data', {})
    print(changed_data, 'changed_data')
    unique_id = changed_data.get('id', None)
    print(unique_id, 'unique_id')
    table_name = data.get('table_name', '')
    # database Connection
    db = DB(database="AmopAlgouatDB", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    dbs = DB(database="common_utils", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        print("Failed to start ram and time calc")
        pass
    try:
        # Ensure unique_id is available
        if unique_id is not None:
            # Prepare the update data
            update_data = {key: value for key, value in changed_data.items() if key != 'unique_col' and key != 'id'}
            # Perform the update operation
            db.update_dict(table_name, update_data, {'id': unique_id})
            print('edited successfully')
            message = f"Data Edited Successfully"
            response_data = {"flag": True, "message": message}
            # End time calculation
            end_time = time()
            time_consumed = end_time - start_time
            audit_data_user_actions = {"service_name": 'Module Management',"created_date": date_started,
            "created_by": username,
                "status": str(response_data['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": json.dumps(changed_data),
                "module_name": "update_superadmin_data","request_received_at":request_received_at
            }
            dbs.update_audit(audit_data_user_actions, 'audit_user_actions')
            return response_data
    except Exception as e:
        print(f"An error occurred: {e}")
        message = f"Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'update_superadmin_data',"created_date": date_started,"error_message": message,"error_type": error_type,"users": username,"session_id": session_id,"tenant_name": Partner,"comments": "","module_name": "Module Managament","request_received_at":request_received_at}
            dbs.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            print(f"Exception is {e}")
        return response
        
        

def dataframe_to_blob(data_frame):
    '''
    Description:The Function is used to convert the dataframe to blob
    '''
    # Create a BytesIO buffer
    bio = BytesIO()
    
    # Use ExcelWriter within a context manager to ensure proper saving
    with pd.ExcelWriter(bio, engine='openpyxl') as writer:
        data_frame.to_excel(writer, index=False)
    
    # Get the value from the buffer
    bio.seek(0)
    blob_data = base64.b64encode(bio.read())
    
    return blob_data
    
    
    
def export(data, max_rows=10):
    '''
    Description:Exports data into an Excel file. It retrieves data based on the module name from the database,
    processes it, and returns a blob representation of the data if within the allowed row limit.
    '''
    # Print the request data for debugging
    print(f"Request Data: {data}")
    ### Extract parameters from the Request Data
    Partner = data.get('Partner', '')
    request_received_at = data.get('request_received_at', None)
    module_name = data.get('module_name', '')
    start_date = data.get('start_date', '')
    end_date = data.get('end_date', '')
    user_name = data.get('user_name', '')
    session_id = data.get('session_id', '')
    ids = data.get('ids', '')
    ##database connection for common utilss
    db = DB(database="common_utils", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        print("Failed to start ram and time calc")
        pass
    try:
        ##databse connenction
        database = DB('AmopAlgouatDB', **db_config)
        # Fetch the query from the database based on the module name
        module_query_df = database.get_data("export_queries", {"module_name": module_name})
        print(module_query_df,'module_query_df')
        ##checking the dataframe is empty or not
        if module_query_df.empty:
            return {
                'flag': False,
                'message': f'No query found for module name: {module_name}'
            }
        # Extract the query string from the DataFrame
        query = module_query_df.iloc[0]['module_query']
        if not query:
            return {
                'flag': False,
                'message': f'Unknown module name: {module_name}'
            }
        ##params for the specific module
        if module_name == "inventory status history" or module_name=="bulkchange status history":
            params=[ids]
        else:
            params = [start_date, end_date]
        ##executing the query
        data_frame = database.execute_query(query, params=params)
        # Check the number of rows in the DataFrame
        row_count = data_frame.shape[0]
        print(row_count,'row_count')
        ##checking the max_rows count
        if row_count > max_rows:
            return {
                'flag': False,
                'message': f'Cannot export more than {max_rows} rows.'
            }
        if module_name=='NetSapiens Customers':
            data_frame['NetSapiensType'] = 'Reseller'
        # Capitalize each word and add spaces
        data_frame.columns = [
            col.replace('_', ' ').title() for col in data_frame.columns
        ]
        data_frame['S.NO'] = range(1, len(data_frame) + 1)
        # Reorder columns dynamically to put S.NO at the first position
        columns = ['S.NO'] + [col for col in data_frame.columns if col != 'S.NO']
        data_frame = data_frame[columns]
        # Proceed with the export if row count is within the allowed limit
        data_frame = data_frame.astype(str)
        data_frame.replace(to_replace='None', value='', inplace=True)

        blob_data = dataframe_to_blob(data_frame)
        # End time calculation
        end_time = time()
        time_consumed = end_time - start_time
        # Return JSON response
        response = {
            'flag': True,
            'blob': blob_data.decode('utf-8')
        }
        audit_data_user_actions = {
            "service_name": 'Module Management',
            "created_date": date_started,
            "created_by": user_name,
            "status": str(response['flag']),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": "",
            "module_name": "export","request_received_at":request_received_at
        }
        db.update_audit(audit_data_user_actions, 'audit_user_actions')
        return response
    except Exception as e:
        error_type = str(type(e).__name__)
        print(f"An error occurred: {e}")
        message = f"Error is {e}"
        response = {"flag": False, "message": message}
        try:
            # Log error to database
            error_data = {
                "service_name": 'Module Management',
                "created_date": date_started,
                "error_message": message,
                "error_type": error_type,
                "users": user_name,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "export","request_received_at":request_received_at
            }
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            print(f"Exception is {e}")
        return response
        
        
        


def inventory_dropdowns_data(data):
    '''
    Retrieves dropdown data based on the given parameters and returns a response with rate plan and communication plan lists.

    Parameters:
    - data (dict): Dictionary containing the dropdown type and list view data ID.

    Returns:
    - dict: A dictionary with a flag indicating success and lists of rate plans and communication plans.
    '''
    try:
        # Initialize database connection
        database = DB('AmopAlgouatDB', **db_config)
        request_received_at = data.get('request_received_at', None)
        # Extract parameters from the input data
        dropdown=data.get('dropdown','')
        # Retrieve service provider ID based on the list view data ID
        list_view_data_id=data.get('list_view_data_id','')
        list_view_data_id=int(list_view_data_id)
        service_provider_id=database.get_data("sim_management_inventory",{'id':list_view_data_id},['service_provider_id'])['service_provider_id'].to_list()[0]
        # Determine the appropriate rate plan list based on the dropdown type
        if dropdown=='Carrier Rate Plan':
            rate_plan_list=database.get_data("carrier_rate_plan",{'service_provider_id':service_provider_id},['rate_plan_code'])['rate_plan_code'].to_list()
        else:
            rate_plan_list=database.get_data("customerrateplan",{'service_provider_id':service_provider_id},['rate_plan_name'])['rate_plan_name'].to_list()
        # Retrieve the list of communication rate plans
        communication_rate_plan_list=database.get_data("jasper_communication_plan",{'service_provider_id':service_provider_id},['communication_plan_name'])['communication_plan_name'].to_list()
        # Prepare and return the response
        message=f"Dropdown data fetched successfully"
        response={"flag":True,"rate_plan_list":rate_plan_list,"communication_rate_plan_list":communication_rate_plan_list,"message":message}
        return response
    except Exception as e:
        message=f"Getting Issue while fetching the data"
        response={"flag":False,"message":message}
        print(f"Exception is {e}")
        return response
        
        
        


def update_inventory_data(data):
    '''
    Description:updates inventory data for a specified module by checking user and tenant to get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view, handles errors, and logs relevant information.
    '''
    print(f"Request Data: {data}")
    Partner = data.get('Partner', '')
    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        print(f"got exception in the restriction")
    ##extract the necessary parameters from the Request Data
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', '')
    module_name = data.get('module_name', '')
    change_event_type = data.get('change_event_type', '')
    changed_data = data.get('changed_data', {})
    print(changed_data, 'changed_data')
    unique_id = changed_data.get('id', None)
    print(unique_id, 'unique_id')
    table_name = data.get('table_name', '')
    # database Connections
    db = DB(database="AmopAlgouatDB", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    dbs = DB(database="common_utils", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        print("Failed to start ram and time calc")
        pass
    try:
        # Ensure unique_id is available
        if unique_id is not None:
            # Prepare the update data
            update_data = {key: value for key, value in changed_data.items() if key != 'unique_col' and key != 'id'}
            # Perform the update operation
            db.update_dict(table_name, update_data, {'id': unique_id})
            history = data.get('history', '')
            try:
                # Call the method to insert the dict in the action history table
                db.insert_dict(history, 'sim_management_inventory_action_history')
            except Exception as e:
                print(f"Exception is {e}")
            message = f"Data Edited Successfully and Inserted in the Action History Table Successfully"
            response_data = {"flag": True, "message": message}
            # End time calculation
            end_time = time()
            time_consumed = end_time - start_time
            audit_data_user_actions = {"service_name": 'Module Management',"created_date": date_started,
            "created_by": username,
                "status": str(response_data['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": json.dumps(changed_data),
                "module_name": "update_superadmin_data","request_received_at":request_received_at
            }
            dbs.update_audit(audit_data_user_actions, 'audit_user_actions')
            return response_data
    except Exception as e:
        print(f"An error occurred: {e}")
        message = f"Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'update_superadmin_data',"created_date": date_started,"error_message": message,"error_type": error_type,"users": username,"session_id": session_id,"tenant_name": Partner,"comments": "","module_name": "Module Managament","request_received_at":request_received_at}
            dbs.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            print(f"Exception is {e}")
        return response
        
        
def get_status_history(data):
    '''
    Retrieves the status history of a SIM management inventory item based on the provided ID.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_id' for querying the status history.

    Returns:
    - dict: A dictionary containing the status history data, header mapping, and a success message or an error message.
    '''
    ##fetching the request data
    list_view_data_id=data.get('list_view_data_id','')
    list_view_data_id=int(list_view_data_id)
    request_received_at = data.get('request_received_at', None)
    try:
        ##Database connection
        database = DB('AmopAlgouatDB', **db_config)
         # Fetch status history data from the database
        sim_management_inventory_action_history_dict=database.get_data("sim_management_inventory_action_history",{'sim_management_inventory_id':list_view_data_id},["service_provider","iccid","msisdn","customer_account_number","customer_account_name","previous_value","current_value","date_of_change","change_event_type","changed_by"]).to_dict(orient='records')
        # Helper function to serialize datetime objects to strings
        def serialize_dates(data):
            for key, value in data.items():
                if isinstance(value, datetime):
                    data[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            return data
        
        # Apply date serialization to each record
        sim_management_inventory_action_history_dict = [
            serialize_dates(record) for record in sim_management_inventory_action_history_dict
        ]
        ##Fetching the header map for the inventory status history
        headers_map=get_headers_mapping(["inventory status history"],"role_name","username","main_tenant_id","sub_parent_module","parent_module")
        message = f"Status History data Fetched Successfully"
        # Prepare success response
        response={"flag":True,"status_history_data":sim_management_inventory_action_history_dict,"header_map":headers_map,"message":message}
        return response
    except Exception as e:
        print(f"Exception is {e}")
        message = f"Unable to Fetch the data"
        response = {"flag": False, "message": message}
        return response        
