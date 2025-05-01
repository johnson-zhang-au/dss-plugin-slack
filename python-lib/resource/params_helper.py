from typing import Any, Dict, List
import dataiku

def list_projects_with_answers_webapp() -> Dict[str, List[Dict[str, str]]]:
    client = dataiku.api_client()
    projects = client.list_projects()  # Assuming `client` is already defined
    project_choices: List[Dict[str, str]] = []
    
    for project_info in projects:
        project = client.get_project(project_info['projectKey'])
        webapps = project.list_webapps()

        # Check if any webapp in the project has the specific type
        if any(webapp.get('type') == 'webapp_document-question-answering_document-intelligence-explorer' for webapp in webapps):
            project_key = project_info['projectKey']
            project_name = project_info.get('name', 'Unknown')
            project_choices.append({"value": project_key, "label": f"{project_name} ({project_key})"})

    return {"choices": project_choices}

def list_webapps_from_project(config: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    # Get the project key from config
    answers_project_key = config.get("answers_project_key",None)
    if answers_project_key == None:
       return {
            "choices": [
                {
                    "value": "wrong",
                    "label": f"Choose the Answers project first",
                }
            ]
        }
    client = dataiku.api_client()
    # Retrieve the project instance using the project key
    project = client.get_project(answers_project_key)
    
    # Retrieve the list of webapps in the project and filter by type
    webapp_choices = [
        {"value": webapp["id"], "label": webapp["name"]}
        for webapp in project.list_webapps()
        if webapp["type"] == 'webapp_document-question-answering_document-intelligence-explorer'
    ]
    
    # Return the choices in the specified structure
    return {"choices": webapp_choices}

def list_groups_with_details() -> Dict[str, List[Dict[str, str]]]:
    """
    Lists all DSS groups and returns them in a format similar to list_projects_with_answers_webapp.
    
    :return: A dictionary with a "choices" key containing a list of group details.
    """
    client = dataiku.api_client()
    groups = client.list_groups()
    group_choices: List[Dict[str, str]] = []

    for group in groups:
        group_name = group.get('name', 'Unknown')
        is_admin = group.get('admin', False)
        group_label = f"{group_name} (Admin)" if is_admin else group_name

        group_choices.append({
            "value": group_name,  # The group's name to be used as a value
            "label": group_label  # A user-friendly label
        })

    return {"choices": group_choices}

def list_available_llms() -> Dict[str, List[Dict[str, str]]]:
    """
    Lists all LLMs available to the Dataiku project.
    
    :return: A dictionary with a "choices" key containing a list of LLM details.
    """
    client = dataiku.api_client()
    project = client.get_default_project()
    llms = project.list_llms()
    llm_choices: List[Dict[str, str]] = []

    for llm in llms:
        llm_id = llm.get('id', '')
        friendly_name = llm.get('friendlyName', 'Unknown LLM')
        
        llm_choices.append({
            "value": llm_id,  # The LLM's ID to be used as a value
            "label": friendly_name  # The friendly name as a label
        })

    return {"choices": llm_choices}

def do(payload, config, plugin_config, inputs):
    parameter_name = payload["parameterName"]

    if parameter_name == "answers_project_key":
        return list_projects_with_answers_webapp()
    elif parameter_name == "answers_webapp_id":
        return list_webapps_from_project(config)
    elif parameter_name == "authorized_dss_group":
        return list_groups_with_details()
    elif parameter_name == "llm_id":
        return list_available_llms()
    else:
        return {
            "choices": [
                {
                    "value": "wrong",
                    "label": f"Problem getting the name of the parameter.",
                }
            ]
        }