var express = require('express');
var bodyParser = require('body-parser');
var http = require('http');

var app = express();

app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.text())                                    
app.use(bodyParser.json({ type: 'application/json'}))  

// Load the SDK for JavaScript
var AWS = require('aws-sdk');
// Set the region 
AWS.config.update({region: 'us-west-2'});


// Create DynamoDB Service Object
var ddbObj = new AWS.DynamoDB({apiVersion: '2012-10-08'})
var docClient = new AWS.DynamoDB.DocumentClient();


/**
 * Create User Table on Schema with Global index
 * -> Check if table is created or not? if not then create else return message
 */
app.get('/createTable', (req, res) => {
    // Creating Table using Schema 
    var params = {
        KeySchema: [
            {
                AttributeName: 'USER_ID',
                KeyType: 'HASH'
            }
        ],
        AttributeDefinitions: [
            {
                AttributeName: 'USER_ID',
                AttributeType: 'S'
            },
            {
                AttributeName: 'USER_NAME',
                AttributeType: 'S'
            },
            {
                AttributeName: 'EMAIL',
                AttributeType: 'S'
            }
        ],
        GlobalSecondaryIndexes: [
            {
                IndexName: 'usernameindex',
                KeySchema: [
                    {
                        AttributeName: 'USER_NAME',
                        KeyType: 'HASH'
                    }
                ],
                Projection: {
                    ProjectionType: "ALL"
                },
                ProvisionedThroughput: {
                    ReadCapacityUnits: 1,
                    WriteCapacityUnits: 1
                }
            },
            {
                IndexName: 'emailindex',
                KeySchema: [
                    {
                        AttributeName: 'EMAIL',
                        KeyType: 'HASH'
                    }
                ],
                Projection: {
                    ProjectionType: "ALL"
                },
                ProvisionedThroughput: {
                    ReadCapacityUnits: 1,
                    WriteCapacityUnits: 1
                }
            }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 1
        },
        TableName: 'USER_INFO'
    };

    // Listing Tables
    ddbObj.listTables({Limit: 100}, (err, data) => {
        if (err) res.send(err);
        else {
            console.log('list table', data);
            if(data.TableNames && data.TableNames.length) {
                const exist = data.TableNames.filter((e) => {return e === 'USER_INFO'}).length > 0;
                if (!exist) {
                    // Call DynamoDB to create the table
                    ddbObj.createTable(params, (err, data) => {
                        if (err) res.send(err);
                        else res.json({'message':'Table Created SuccessFully!!'});
                    });
                } else {
                    res.json({'message':'Table Already Exist!!'});
                }
            } else {
                // Call DynamoDB to create the table
                ddbObj.createTable(params, (err, data) => {
                    if (err) res.send(err);
                    else res.json({'message':'Table Created SuccessFully!!'});
                });
            }
        } 
    });
});


/**
 * Create Dept Table With Local Indexes
 */
app.get('/createDeptTable', (req, res) => {
    // Creating Table using Schema 
    var params = {
        KeySchema: [
            {
                AttributeName: 'DEPT_ID',
                KeyType: 'HASH'
            },
            {
                AttributeName: 'USER_ID',
                KeyType: 'RANGE'
            }
        ],
        AttributeDefinitions: [
            {
                AttributeName: 'DEPT_ID',
                AttributeType: 'S'
            },
            {
                AttributeName: 'USER_ID',
                AttributeType: 'S'
            },
            {
                AttributeName: 'DEPT_NAME',
                AttributeType: 'S'
            }
        ],
        LocalSecondaryIndexes: [
            {
                IndexName: 'deptnameindex',
                KeySchema: [
                    {
                        AttributeName: 'DEPT_ID',
                        KeyType: 'HASH'
                    },
                    {
                        AttributeName: 'DEPT_NAME',
                        KeyType: 'RANGE'
                    }
                ],
                Projection: {
                    ProjectionType: "ALL"
                }
            }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 1
        },
        TableName: 'DEPT_INFO'
    };

    // Listing Tables
    ddbObj.listTables({Limit: 100}, (err, data) => {
        if (err) res.send(err);
        else {
            console.log('list table', data);
            if(data.TableNames && data.TableNames.length) {
                const exist = data.TableNames.filter((e) => {return e === 'DEPT_INFO'}).length > 0;
                if (!exist) {
                    // Call DynamoDB to create the table
                    ddbObj.createTable(params, (err, data) => {
                        if (err) res.send(err);
                        else res.json({'message':'Table Created SuccessFully!!'});
                    });
                } else {
                    res.json({'message':'Table Already Exist!!'});
                }
            } else {
                // Call DynamoDB to create the table
                ddbObj.createTable(params, (err, data) => {
                    if (err) res.send(err);
                    else res.json({'message':'Table Created SuccessFully!!'});
                });
            }
        } 
    });
})


/**
 * Get Table List
 */
app.get('/listTable', (req, res) => {
    ddbObj.listTables({Limit: 100}, (err, data) => {
        if (err) res.send(err);
        else res.json(data);
    });
    
    
});

/**
 * Delete Table
 */
app.post('/deleteTable', (req, res) => {
    var params ={
        TableName: req.body.tableName
    };
    // Deleting Table
    ddbObj.deleteTable(params, (err, data) => {
        if (err) res.send(err);
        else res.json(data);
    });
});

/**
 * Descibe Structure of Table
 */
app.get('/describeTable/:tableName', (req, res) => {
    var params = {
        TableName: req.params.tableName
    };
    ddbObj.describeTable(params, (err, data) => {
        if (err) res.send(err);
        else res.json(data);
    });
});


/**
 * Insert Data in User Table
 */
app.post('/insertData', (req, res) => {
    var params = {
        TableName: "USER_INFO",
        Item: {
            "USER_ID": {"S": req.body.user_id},
            "USER_NAME": {"S": req.body.user_name},
            "FIRST_NAME": {"S": req.body.first_name},
            "LAST_NAME": {"S": req.body.last_name},
            "EMAIL": {"S": req.body.email},
            "CONTACT_NO": {"N": req.body.contact_no}
        }
    };
    console.log(params);
    /**
    * Writing Data in Table
    */
   ddbObj.putItem(params, (err, data) => {
        if(err) res.send(err);
        else res.json(data);
    });
});


/**
 * Insert Data in Dept Table
 */
app.post('/insertDeptData', (req, res) => {
    var data = req.body.deptData;
    var putReq = [];
    if (data && data.length) {
        data.forEach(element => {
            putReq.push({
                PutRequest: {
                    Item: {
                        "DEPT_ID": {"S": element.dept_id},
                        "USER_ID": {"S": element.user_id},
                        "DEPT_NAME": {"S": element.dept_name}
                    }
                }
            });
        });
    }
    var params = {
        RequestItems: {
            "DEPT_INFO": putReq
        }
    };
    console.log(params);
    /**
    * Writing Data in Table
    */
   ddbObj.batchWriteItem(params, (err, data) => {
        if(err) res.send(err);
        else res.json(data);
    });
});

/**
 * Get User List
 */
app.get('/getUserList', (req, res) => {
    var params = {
        TableName: "USER_INFO",
        ProjectionExpression: "USER_ID, USER_NAME, EMAIL, CONTACT_NO"
    };
    docClient.scan(params, (err, data) => {
        if (err) res.send(err);
        else res.json(data);
    });
});


/**
 * Get Data from Multiple Tables / batch get
 */
app.get('/getBatchUser', (req, res) => {
    var params = {
        RequestItems: {
          'USER_INFO': {
            Keys: [
              {'USER_ID': '1'}
            ]
          },
          'DEPT_INFO': {
            Keys: [
              {'DEPT_ID': '1', 'USER_ID': '1' }
            ]
          }
        }
    };
    docClient.batchGet(params, (err, data) => {
        if (err) res.send(err)
        else res.json(data)
    });
});


/**
 * Get User By Id
 */
app.get('/getUserById/:userID', (req, res) => {
    var params = {
        TableName: "USER_INFO",
        ProjectionExpression: "USER_ID, USER_NAME, EMAIL, CONTACT_NO",
        Key: {
            "USER_ID": req.params.userID
        }
    };
    docClient.get(params, (err, data) => {
        if (err) res.send(err);
        else res.json(data);
    });
});

/**
 * Get User by Name using username global index
 */
app.get('/getUserByName/:userName', (req, res) => {
    var params = {
        TableName: "USER_INFO",
        "IndexName": "usernameindex",
        KeyConditionExpression: "#username = :username",
        ExpressionAttributeNames: {
            "#username": "USER_NAME"
        },
        ExpressionAttributeValues: {
            ":username": req.params.userName
        }
    };
    docClient.query(params, (err, data) => {
        if (err) res.send(err)
        else res.json(data)
    });
});


/**
 * Get User Data with Fliter Expression 
 */
app.get('/searchUser/:searchTerm', (req, res) => {
    var params = {
        TableName: "USER_INFO",
        "IndexName": "usernameindex",
        FilterExpression: "(contains(#username, :searchTerm)  OR contains(#firstName, :searchTerm) OR contains(#lastName, :searchTerm) OR contains(#email, :searchTerm))",
        ExpressionAttributeNames: {
            "#username": "USER_NAME",
            "#firstName": "FIRST_NAME",
            "#lastName": "LAST_NAME",
            "#email": "EMAIL"
        },
        ExpressionAttributeValues: {
            ":searchTerm": req.params.searchTerm
        }
    };
    docClient.scan(params, (err, data) => {
        if (err) res.send(err)
        else res.json(data);
    });
});


/**
 * Update User Data
 */
app.post('/updateUserById', (req, res) => {
    var params = {
        TableName: "USER_INFO",
        Key: {
            "USER_ID": req.body.user_id
        },
        UpdateExpression: "set USER_NAME= :user_name, EMAIL= :email, CONTACT_NO= :contact_no",
        ExpressionAttributeValues: {
            ":user_name": req.body.user_name,
            ":email": req.body.email,
            ":contact_no": req.body.contact_no
        },
        ReturnValues:"UPDATED_NEW"
    };
    console.log(params);
    docClient.update(params, (err, data) => {
        if (err) res.send(err);
        else res.json(data);
    });
});


/**
 * Get Table Backup
 */
app.get('/takeBackup', (req, res) => {
    var params = {
        BackupName: 'USER_INFO_BKP_16082018',
        TableName: 'USER_INFO'
    };
    ddbObj.createBackup(params, (err, data) => {
        if (err) res.send(err);
        else res.json(data);
    });
});

/**
 * Get Count of User by userName with Global Index
 */
app.get('/getUserCountByName/:userName', (req, res) => {
    var params = {
        TableName: 'USER_INFO',
        IndexName: 'usernameindex',
        KeyConditionExpression: '#username= :username',
        ExpressionAttributeNames: {
            '#username': 'USER_NAME'
        },
        ExpressionAttributeValues: {
            ':username': req.params.userName
        },
        Select: 'COUNT'
    };
    docClient.query(params, (err, data) => {
        if (err) res.send(err);
        else res.json(data);
    });
});


const server = http.createServer(app);
server.on('listening',function(){
	console.log('ok, server is running')
})

server.listen(8080);
module.exports = app;




/* // Get Table Name from Argument
var tableName = {
    TableName: process.argv[2]
};
 */

// Describing Tables



/* var data = [
    {
        "USER_ID": 1,
        "USER_NAME": "vivek.rajyaguru",
        "FIRST_NAME": "Vivek",
        "LAST_NAME": "Rajyaguru",
        "EMAIL": "vivek.rajyaguru@volansys.com",
        "CONTACT_NO": "9408753791"
    },
    {
        "USER_ID": 2,
        "USER_NAME": "test.user",
        "FIRST_NAME": "Test",
        "LAST_NAME": "User",
        "EMAIL": "test.user@volansys.com",
        "CONTACT_NO": "123456789"
    }
]; */
/* data.forEach((user) => {
    var params = {
        TableName: "USER_INFO",
        Item: {
            "USER_ID": user.USER_ID,
            "USER_NAME": user.USER_NAME,
            "FIRST_NAME": user.FIRST_NAME,
            "LAST_NAME": user.LAST_NAME,
            "EMAIL": user.EMAIL,
            "CONTACT_NO": user.CONTACT_NO
        }
    };
});

 */


