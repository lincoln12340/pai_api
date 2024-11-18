import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import pandas as pd
from openai import OpenAI
import os
from dotenv import load_dotenv


load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
google_sheet_url = os.getenv("GOOGLE_SHEET_URL")
private_key = os.getenv("PRIVATE_KEY")
project_id = os.getenv("PROJECT_ID")
private_key_id = os.getenv("PRIVATE_KEY_ID")
client_email = os.getenv("CLIENT_EMAIL")
client_id = os.getenv("CLIENT_ID")
auth_uri = os.getenv("AUTH_URI")
token_uri = os.getenv("TOKEN_URI")
auth_provider_x509_cert_url = os.getenv("AUTH_PROVIDER_X509_CERT_URL")
client_x509_cert_url = os.getenv("CLIENT_X509_CERT_URL")
universe_domain = os.getenv("UNIVERSE_DOMAIN")
vector_store_id = os.getenv("VECTOR_STORE_ID")
google_sheet_scope = os.getenv("GOOGLE_SHEETS_API_SCOPE")

client = OpenAI(api_key=api_key)

app = FastAPI()

# Define the input model to receive row number and webhook URL
class RequestBody(BaseModel):
    row_number: int
    webhook_url: str
    company_name: str
    esg_report: str
    file_id: str
    date: str
    file_open_ai: str


credentials_dict = {
    "type": "service_account",
    "project_id": project_id,
    "private_key_id": private_key_id,
    "private_key": private_key,
    "client_email": client_email,
    "client_id": client_id,
    "auth_uri": auth_uri,
    "token_uri": token_uri,
    "auth_provider_x509_cert_url": auth_provider_x509_cert_url,
    "client_x509_cert_url": client_x509_cert_url,
    "universe_domain": universe_domain,
}

credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    credentials_dict, [google_sheet_scope]
)



# Google Sheets setup
gc = gspread.authorize(credentials)
sh = gc.open_by_url(google_sheet_url)
worksheet = sh.sheet1
print("im here lolool")
# Endpoint to handle data reception and processing
@app.post("/process-row")
def process_row(request_body: RequestBody):
    row_number = request_body.row_number
   

    # Get all values from the sheet and convert them into a DataFrame
    data = worksheet.get_all_values()
    df = pd.DataFrame(data)

    # Set the first row as headers and clean the DataFrame
    df.columns = df.iloc[0]  # First row as headers
    #df = df.drop(0).reset_index(drop=True)  # Drop the first row, reset index
    scope1 = df.iloc[-1,4]
    scope2 = df.iloc[-1,5]
    scope3 = df.iloc[-1,6]
    total_scope = df.iloc[-1,7]
    print(total_scope)

    if total_scope == "Not Reported" and any(value != "Not Reported" for value in [scope1, scope2, scope3]):
    # Gather only reported Scope values (i.e., not "Not Reported")
        reported_values = [str(value) for value in [scope1, scope2, scope3] if value != "Not Reported"]
        # Join reported values as a single string separated by "; "
        total_scope = "; ".join(reported_values)
        # Update the DataFrame with the new projection value
        df.iloc[-1,7 ] = total_scope
    else:
        df.iloc[-1,7]= total_scope

    df_h_onwards = df.iloc[:, 7:19].drop(df.columns[[15, 16]], axis=1) # Select columns from H onwards
    
    in_manu = df.iloc[-1, 19]
    in_manu_bool = True if "Yes" in in_manu else False if "No" in in_manu else None

    comm_prod = df.iloc[-1, 20]
    comm_prod_bool = True if "Yes" in comm_prod else False if "No" in comm_prod else None


    # Ensure the row number is valid
    if (row_number-1) > len(df_h_onwards):
        raise HTTPException(status_code=400, detail= " Invalid row number=====")

    # Process the specific row (H onwards)
    report_l = []
# Process the specific row (H onwards)
    row_h_onwards = df_h_onwards.iloc[row_number-1]
    if request_body.esg_report == "Yes":
        for col, value in row_h_onwards.items():
            val = value.lower()
            if val != "not reported":
                report = f"{col}: {value}"
                # Format the output for better readability
                formatted_report = report.replace(":", ":\n\t").replace(" - ", "\n\t- ")
                report_l.append(formatted_report)
        report_l = summarize_text(client, report_l)
    else: 
        report_l = f"{request_body.company_name} doesn't have an ESG Report yet"
    
 


    not_reported_columns = df_h_onwards.columns[row_h_onwards == "Not Reported"]
    list_nr = not_reported_columns.tolist()
    

    excluded_numbers = {"1.10", "1.11", "1.12", "1.13"}

# Filter numbers, excluding only if they are present in list_nr
    numbers = []

# Filter numbers, excluding only if they are present in list_nr
    for col in list_nr:
        match = re.match(r'^\d+\.\d+', col)
        if match and match.group() not in excluded_numbers:
            print(f"Processing col: {col} -> Match: {match.group()}")
            numbers.append(match.group())
    
    num_reported = 8-len(numbers)

    # Find columns where the value is "Not Reported"
    b_list = ", ".join(numbers)
    text = f"{request_body.company_name} does not report on the following indicators of the Principal Adverse Impact (PAI) on sustainability factors (listed as [table number].[indicator number in table]): {b_list}, 2.5. We will engage with the company to ask why it doesnt report on these indicators and, if suitable, we will request that it starts reporting on them."

    ungc_text = f"Upon analysis of company documents and disclosures, to the funds knowledge {request_body.company_name} has not been involved in violations of the UNGC principles or OECD Guidelines for Multinational Enterprises. Furthermore, the content of the companys Code of Business Conduct and Ethics showcases that the company has in place appropriate policies to monitor compliance with and grievance/complaints handling mechanisms to address violations of UNGC principles or OECD Guidelines for Multinational Enterprises"

    dei = df.iloc[row_number-1,18]
    gender_pay_l = df.iloc[row_number-1,17]
    dei_bool = True
    gender_pay = True
    response = None


    if dei.lower() != "not reported":
        chat_completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are an AI model designed to determine whether the average board of directors female representation is larger, equal to lower than 26%. Only output above, equal or below"
                },
                {
                    "role": "user",
                    "content": f"is the female representation in {request_body.company_name} higher than 26%, use this text to guide {dei} "
                },
            ]
        )
        response = chat_completion.choices[0].message.content
        dei = summarize_female_representation(client,dei)
        dei_message = f"{dei}. Across several studies analyzing female representation in the biopharma industry in US, Europe and Australia, the average board of directors female representation is 26%. {request_body.company_name} is therefore {response} the average "
    else: 
        dei_bool = False
        dei_message = f"[MANUALLY CHECK].Across several studies analyzing female representation in the biopharma industry in US, Europe and Australia, the average board of directors female representation is 26%. {request_body.company_name} is therefore [MANUALLY CHECK] the average"
        

    if gender_pay_l.lower() == "not reported":
        gender_pay = False
        gender_pay_message = f"We could not find information as to gender pay gap analysis in the companys publicly available documents."
    else:
        gender_pay_message = f"The gender pay gap for {request_body.company_name} is {gender_pay_l}"


    # Base statements with variable PAI indicators based on conditions
   # Base statements with variable PAI indicators based on conditions
    base_neg_harm = f"Considering the points just discussed, the fund believes {request_body.company_name} is not at risk of having a negative impact/do harm on the metrics of the following PAI indicators: "
    base_ovr_harm = f"The outcome of the fund’s analysis showed that {request_body.company_name} provides enough information on "

    # Conditionals for PAI indicators based on `gender_pay` and `dei_bool` flags
    if dei_bool:
        if gender_pay:
            neg_harm_indicators = "1.10-1.11-1.12-1.13" if response.lower() != "below" else "1.10-1.11-1.12"
            ovr_harm_indicators = "1.10 -1.11 -1.12 -1.13 and 3.6"
            sufficiency_note = "5 of the 5 very important indicators"
            harm_statement = "does no significant harm for the time being."
        else:
            neg_harm_indicators = "1.10-1.11-1.13. We cannot conclude on 1.12"
            ovr_harm_indicators = "1.10 -1.11 -1.13 and 3.6"
            sufficiency_note = "4 of the 5 very important indicators"
            harm_statement = "does no significant harm for the time being."
    else:
        if gender_pay:
            neg_harm_indicators = "1.10-1.11-1.12. We cannot conclude on 1.13" if response.lower() != "below" else "1.10-1.11-1.12"
            ovr_harm_indicators = "1.10 -1.11 -1.12 and 3.6"
            sufficiency_note = "4 of the 5 very important indicators"
            harm_statement = "does no significant harm for the time being."
        else:
            neg_harm_indicators = "1.10-1.11. We cannot conclude on 1.12 - 1.13"
            ovr_harm_indicators = "1.10 -1.11 and 3.6"
            sufficiency_note = "3 of the 5 very important indicators"
            harm_statement = "does significant harm for the time being."  # Set for < 4 indicators

    # Construct final statements
    neg_harm = f"{base_neg_harm}{neg_harm_indicators}"
    ovr_harm = f"{base_ovr_harm}{sufficiency_note} identified by the fund ({ovr_harm_indicators}). The fund identifies {request_body.company_name} as a company that {harm_statement}"

    business_conduct = f"In the company’s code of business conduct and ethics, {request_body.company_name} states its communication channels put forward to report concerns/seek advice and encourage its employees to be proactive in that regard. It includes a hotline for confidential and anonymous reporting. Therefore, we believe {request_body.company_name} provides sufficient whistleblower protection and therefore does not have a negative impact on the indicator 3.6 of the PAI on sustainability factors, that was chosen by the fund as a relevant indicator to monitor for portfolio companies."

    manu_com = evaluate_company(in_manu_bool,comm_prod_bool,num_reported,request_body.company_name,b_list)


    if harm_statement == "does no significant harm for the time being." and "we cannot conclude" not in manu_com:
        sustainability_harm = f"{request_body.company_name} contributes to developing medicines for high unmet needs, and its governance documents outline business conduct aligned with the values of the fund. Therefore, the fund identifies {request_body.company_name} as a sustainable investment."
    else:
        sustainability_harm = f"{request_body.company_name} contributes to developing medicines for high unmet needs, and its governance documents outline business conduct aligned with the values of the fund. However, it might be at risk of causing significant harm per the funds DNSH analysis. Therefore, the fund identifies {request_body.company_name} as not a sustainable investment."
    

    # Post the numbers to the provided webhook
    response = requests.post(request_body.webhook_url, json={"ESG Report": report_l, "Not Reported Indicators": text, "UNGC": ungc_text, "Board Members": dei_message, "Gender Pay": gender_pay_message, "Negative Harm": neg_harm, "Business Conduct ": business_conduct, "Overall": ovr_harm, "Sustainability": sustainability_harm,"Manufacturing": manu_com, "Row_number": row_number, "File Id": request_body.file_id, "Company Name": request_body.company_name, "Date": request_body.date})
    # Check if the webhook post was successful
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to post to the webhook")
    
    deleted_vector_store_file = client.beta.vector_stores.files.delete(
        vector_store_id= vector_store_id,
        file_id= request_body.file_open_ai
    )

    response2 = deleted_vector_store_file

    return {"message": "Success", "text": "Testing"}




def summarize_female_representation(client,dei):
    chat_completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": "You are an AI model designed to summarize text with a focus on the board of directors' female representation, highlighting details like the percentage of female members, the female-to-male ratio, and any comparisons relative to the male members."
            },
            {
                "role": "user",
                "content": f"Summarize this text {dei}"
            }
        ]
    )
    return chat_completion.choices[0].message.content



def summarize_text(client,msg):
    chat_completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": "You are an AI model designed to summarize text in a clear, concise manner. Format the summary in bullet points for readability."
            },
            {
                "role": "user",
                "content": f"Summarize this text: {msg}"
            }
        ]
    )
    
    # Capture and format response
    summary = chat_completion.choices[0].message.content
    

    return summary
    


def evaluate_company(commercial_products, own_manufacturing, indicators_reported, company_name,nr_list):
    if not commercial_products and not own_manufacturing:
        # Case 1: Companies that do not have commercial products and do not have their own manufacturing
        return f"The fund believes it is reasonable to expect {company_name} not to be at risk of having a negative impact on the metrics mentioned in these indicators."
    
    elif not commercial_products and own_manufacturing:
        # Case 2: Companies that do not have commercial products but do have their own manufacturing
        return f"Considering the footprint of the company and intensity of its manufacturing activities, we cannot conclude {company_name} is not at risk of doing significant harm based on indicator 1.7."
    
    elif commercial_products and not own_manufacturing:
        # Case 3: Companies that do have commercial products but do not have their own manufacturing
        return f"Considering the footprint of the company and intensity of its manufacturing activities, we cannot conclude {company_name} is not at risk of doing significant harm as it is responsible for manufacturing via third parties."
    
    elif commercial_products and own_manufacturing:
        # Cases 4 & 5: Companies with commercial products and own manufacturing, depending on indicators reported
        if indicators_reported < 5:
            # Case 4: Companies with commercial products and own manufacturing, reporting on less than 5 indicators
            return f"Considering the footprint of the company and intensity of its manufacturing activities, we cannot conclude {company_name} is not at risk of doing significant harm based on all indicators. We will engage with the company to understand why it does not report on indicator {nr_list}."
        else:
            # Case 5: Companies with commercial products and own manufacturing, reporting on 5 or more indicators
            return f"Considering the company operates manufacturing facilities but reports on 5 or more metrics, we can reasonably rule out {company_name} being at risk of negative impact on the metrics mentioned in these indicators."

# Example usage
# company_scenario = evaluate_company(commercial_products=True, own_manufacturing=True, indicators_reported=4)
# print(company_scenario)

