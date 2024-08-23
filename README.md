# offen-petroleum.app-ns-saved-search
Absolutely, let's break down the process of creating, deploying, and using RESTlets in NetSuite:

**1. Creation**

* **Navigate to the Scripting Area:** In your NetSuite account, go to Customization > Scripting > Scripts > New.
* **Select RESTlet Type:** Choose "RESTlet" as the script type.
* **Write Your Script:**  Develop the SuiteScript code for your RESTlet. Key components:
    * `get` function: Handles GET requests (data retrieval).
    * `post` function: Handles POST requests (data submission).
    * `put` function: Handles PUT requests (data update).
    * `delete` function: Handles DELETE requests (data removal).
    * Each function should process requests, interact with NetSuite data (using search, load, create, update, delete operations), and return appropriate responses.
* **Save & Deploy:** Save your script. Then, deploy it:
    * Go to Customization > Scripting > Script Deployments > New.
    * Select your RESTlet script and define deployment parameters (audience, status, authentication, etc.).
    * Upon saving the deployment, NetSuite will provide you with URLs for accessing your RESTlet.

**2. Deployment**

* **Script File Upload (Optional):** If using SuiteCloud Development Framework (SDF), manage your RESTlet script as part of a file-based project.
* **Script Record Creation:** Create a script record for your RESTlet, providing a meaningful ID.
* **Script Deployment Record Creation:** Create a script deployment record, specifying details like audience and authentication.
* **URL Generation:** NetSuite automatically generates partial and full URLs for accessing your RESTlet. The full URL incorporates your NetSuite account's specific RESTlet domain.

**3. Getting Data (API Interaction)**

* **Construct the Request:**
    * Use the full URL provided by NetSuite.
    * Choose the appropriate HTTP method (GET, POST, PUT, DELETE) based on your desired action.
    * Include required headers:
        * `Content-Type`: Typically 'application/json'
        * `Authorization`: For authentication (details depend on your chosen authentication method in the deployment)
    * For POST/PUT requests, include the request data in the body (usually JSON format).
* **Send the Request:** Use your preferred tool (e.g., Postman, cURL, programming language's HTTP libraries) to send the constructed request.
* **Handle the Response:**
    * NetSuite will return a response, usually in JSON format.
    * Process the response data as needed in your application.

**Example (GET request using cURL)**

```bash
curl -X GET \
  -H 'Content-Type: application/json' \
  -H 'Authorization: <Your_Authentication_Header>' \
  'https://<Your_NetSuite_Account_ID>.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=<Your_Script_ID>&deploy=<Your_Deployment_ID>'
```

**Important Considerations**

* **Authentication:** Choose the appropriate authentication method for your RESTlet (e.g., OAuth 2.0, Token-based Authentication, Basic Authentication) and ensure your requests include the necessary credentials.
* **Error Handling:** Implement robust error handling in your RESTlet and in your API-consuming application to gracefully manage potential issues.
* **Testing:** Thoroughly test your RESTlet using various scenarios to ensure it functions correctly and securely.

**Remember:** RESTlets provide powerful customization and integration capabilities in NetSuite. Use them judiciously and follow best practices for development, deployment, and security. 

Feel free to ask if you have any further questions or need more specific guidance on any part of this process! 
