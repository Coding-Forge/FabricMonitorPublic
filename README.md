# Introduction 
This project looks to take advantage of all the work put into the original Power BI Monitor application and port it to Fabric. This project has several components including a means of containerizing the application and for those enthusiasts that like to use Jupyter Notebooks a whole section is devoted to "BUILDING" the application but using notebooks.

# Getting Started
To use this application you will need to [create a service principal](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app) in Microsoft Entra. This service principal will need to be granted specific permissions within the [Fabric Admin Portal](https://app.fabric.microsoft.com/admin-portal) and for those that would also like to capture information about users licenses and other privileged information will have to grant permissions to Microsoft Graph.

## Fabric Admin Portal Settings  
You can grant specific permissions to service principals to use Fabric APIs including getting metadata of your Power BI semantic models. To enable this the service principal must be put into a group and that group will then be given permission to access the Fabric APIs

<img src="images/admin-portal-settings.png" heigth="500px" width="500px"></img>

## Service Principal API permissions  
In order for the service principal to leverage all of the functionality of the Fabric Montior application you will will need to grant specific API permissions for the service principal. The following can be granted when editing the Service Principal in Microsoft Entra under the Apps section and API permissions.

### Graph Permissions  
|API/Permissions Name|Type|Description|  
|---|---|---|
|Directory.Read.All|Application|Read Directory Data|
|User.Read|Delegated|Sign in and read user profile|  
|User.Read.All|Application|Read all user's full profile

### Power BI  
|API/Permissions Name|Type|Description|  
|---|---|---|
|Tenant.Read.All|Delegated|View all content in tenant|

<img src="images/Service-Principal-API-Permissions.png" height="400px" width="1500px"></img>


TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)