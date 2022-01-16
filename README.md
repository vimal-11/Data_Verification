# Data Verification Robotic Process Automation
Validation of user data from registration forms by Robotic Process Automation in python  
  
Automating the human data verification process by processing the documents and files submitted by the user to verify user's credentials in the registration form.
  
## Run Project 
  
  
### Installing Requirements  
  
      pip install -r requirements.txt   
        
### Driver Code  
`main.py`
  
    
### Required Arguments

> sys.argv[1] = name of the candidate  
> sys.argv[2] = date of birth  
> sys.argv[3] = certificate document (path)  

### Command  
  
      python main.py <name> <date_of_birth> <path_of_document>
        
#### Example  
  
      python main.py example 01-01-2022 meta/test_db.txt
