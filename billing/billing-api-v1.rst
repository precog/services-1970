===========================
The ReportGrid Accounts API
===========================

.. contents:: API Methods

--------
Overview
--------

The accounts api provides the following functionality:

- create account
- get account
- update account (not currently available)
- close account (not currently available)
- run audit (internal use only)
- run assessment (internal use only)

--------------------
Fundamental Concepts
--------------------

Customer Identity
=================

A customer account has two principal identifiers the customer signup email and the customer account token.

Contact Information
===================

Represents customers contact info and per the user interface all of the fields are required.

Service Information
===================

Represents the customers subscribed and plan and various stats about the state of the account. Other than planId
all of these attributes are read-only.

Billing Information
===================

Represents the specific information required for billing and is optional.

-----------
API Methods
-----------

//////
////// Quick Services Docs
//////


/accounts/ - post (Create new account)

Request: 
{
  "email":"a@b.com",
  "password":"abc123",
  "planId":"starter",
  "planCreditOption":"developer", [optional]
  "contact":{
    "firstName":"John",
    "lastName":"Doe",
    "company":"b co",
    "title":"CEO",
    "phone":"411",
    "website":"b.com",
    "address":{
      "street":"123 Street",
      "city":"Broomfield",
      "state":"CO",
      "postalCode":"60607"
    }
  },
  "billing":{ [optional]
    "cardholder":"George Harmon",
    "number":"4111111111111111",
    "expMonth":5,
    "expYear":2012,
    "cvv":"123",
    "billingPostalCode":"60607"
  }
}

Response: 
{
  "id":{
    "email":"a@b.com",
    "tokens":{
      "production":"675C5D06-3880-45D9-8423-C0E778920C4B",
      "development":"B51B1627-F58F-4DAB-B9EB-E6E72D8A6B86"
    }
  },
  "contact":{
    "firstName":"John",
    "lastName":"Doe",
    "company":"b co",
    "title":"CEO",
    "phone":"411",
    "website":"b.com",
    "address":{
      "street":"123 Street",
      "city":"Broomfield",
      "state":"CO",
      "postalCode":"60607"
    }
  },
  "service":{
    "planId":"starter",
    "accountCreated":1321553329267,
    "credit":25000,
    "lastCreditAssessment":1321488000000,
    "usage":0,
    "status":"ACTIVE",
    "gracePeriodExpires":null,
    "subscriptionId":null
  },
  "billing":{
    "cardholder":"George Harmon",
    "number":"************1111",
    "expMonth":5,
    "expYear":2012,
    "billingPostalCode":"60607"
  }
}


/accounts/close - post (Close account)

Request: 
{
  "email":"g@h.com",
  "password":"abc123"
}

Response: 
{
  "id":{
    "email":"g@h.com",
    "tokens":{
      "production":"5142D487-CF48-4282-A0DB-E79ECCE8222F",
      "development":"146C4A1F-E654-4EBD-AEC5-A5225DDCBB85"
    }
  },
  "contact":{
    "firstName":"",
    "lastName":"",
    "company":"",
    "title":"",
    "phone":"",
    "website":"",
    "address":{
      "street":"",
      "city":"",
      "state":"",
      "postalCode":"30000"
    }
  },
  "service":{
    "planId":"bronze",
    "accountCreated":1321553473647,
    "credit":0,
    "lastCreditAssessment":1321488000000,
    "usage":0,
    "status":"ACTIVE",
    "gracePeriodExpires":null,
    "subscriptionId":"3s4p5g"
  },
  "billing":{
    "cardholder":"George Harmon",
    "number":"************1111",
    "expMonth":5,
    "expYear":2012,
    "billingPostalCode":"60607"
  }
}


/accounts/get - post (Get account - deprecated)

Request:
{
  "email":"g@h.com",
  "password":"abc123"
}

Response:
{
  "id":{
    "email":"g@h.com",
    "tokens":{
      "production":"5142D487-CF48-4282-A0DB-E79ECCE8222F",
      "development":"146C4A1F-E654-4EBD-AEC5-A5225DDCBB85"
    }
  },
  "contact":{
    "firstName":"",
    "lastName":"",
    "company":"",
    "title":"",
    "phone":"",
    "website":"",
    "address":{
      "street":"",
      "city":"",
      "state":"",
      "postalCode":"30000"
    }
  },
  "service":{
    "planId":"bronze",
    "accountCreated":1321553473647,
    "credit":0,
    "lastCreditAssessment":1321488000000,
    "usage":0,
    "status":"ACTIVE",
    "gracePeriodExpires":null,
    "subscriptionId":"3s4p5g"
  },
  "billing":{
    "cardholder":"George Harmon",
    "number":"************1111",
    "expMonth":5,
    "expYear":2012,
    "billingPostalCode":"60607"
  }
}


/accounts/info/ - put (Update info)

Request: 
{
  "authentication":{
    "email":"g@h.com",
    "password":"abc123"
  },
  "newEmail":"new@email.com", [optional]
  "newPassword":{             [optional]
    "password":"newPassword",
    "confirmPassword":"newPassword"
  },
  "newPlanId":"gold",         [optional]
  "newContact":{              [optional]
    "firstName":"John",
    "lastName":"Doe",
    "company":"b co",
    "title":"CEO",
    "phone":"411",
    "website":"b.com",
    "address":{
      "street":"123 Street",
      "city":"Broomfield",
      "state":"CO",
      "postalCode":"60607"
    }
  }
}

Response: 
{
  "id":{
    "email":"new@email.com",
    "tokens":{
      "production":"0AB9B4B0-E0C5-4D0E-82C1-1CFA864FA05A",
      "development":"F322F82F-8C46-4E61-B99D-25F4C48E6698"
    }
  },
  "contact":{
    "firstName":"John",
    "lastName":"Doe",
    "company":"b co",
    "title":"CEO",
    "phone":"411",
    "website":"b.com",
    "address":{
      "street":"123 Street",
      "city":"Broomfield",
      "state":"CO",
      "postalCode":"60607"
    }
  },
  "service":{
    "planId":"gold",
    "accountCreated":1321553410058,
    "credit":0,
    "lastCreditAssessment":1321488000000,
    "usage":0,
    "status":"ACTIVE",
    "gracePeriodExpires":null,
    "subscriptionId":"2stdn6"
  }
}


/accounts/info/get - post (Get info)

Request: 
{
  "authentication":{
    "email":"g@h.com",
    "password":"abc123"
  },
  "newEmail":"new@email.com",
  "newPassword":{
    "password":"newPassword",
    "confirmPassword":"newPassword"
  },
  "newPlanId":"gold",
  "newContact":{
    "firstName":"John",
    "lastName":"Doe",
    "company":"b co",
    "title":"CEO",
    "phone":"411",
    "website":"b.com",
    "address":{
      "street":"123 Street",
      "city":"Broomfield",
      "state":"CO",
      "postalCode":"60607"
    }
  }
}

Response: 
{
  "id":{
    "email":"new@email.com",
    "tokens":{
      "production":"0AB9B4B0-E0C5-4D0E-82C1-1CFA864FA05A",
      "development":"F322F82F-8C46-4E61-B99D-25F4C48E6698"
    }
  },
  "contact":{
    "firstName":"John",
    "lastName":"Doe",
    "company":"b co",
    "title":"CEO",
    "phone":"411",
    "website":"b.com",
    "address":{
      "street":"123 Street",
      "city":"Broomfield",
      "state":"CO",
      "postalCode":"60607"
    }
  },
  "service":{
    "planId":"gold",
    "accountCreated":1321553410058,
    "credit":0,
    "lastCreditAssessment":1321488000000,
    "usage":0,
    "status":"ACTIVE",
    "gracePeriodExpires":null,
    "subscriptionId":"2stdn6"
  }
}


/accounts/billing/ - put (Update billing)

Request: 
{
  "authentication":{
    "email":"a@b.com",
    "password":"abc123"
  },
  "billing":{
    "cardholder":"George Harmon",
    "number":"4111111111111111",
    "expMonth":5,
    "expYear":2012,
    "cvv":"123",
    "billingPostalCode":"60607"
  }
}

Response: 
{
  "cardholder":"George Harmon",
  "number":"************1111",
  "expMonth":5,
  "expYear":2012,
  "billingPostalCode":"60607"
}


/accounts/billing/get - post (Get billing info)

Request: 
{
  "authentication":{
    "email":"a@b.com",
    "password":"abc123"
  },
  "billing":{
    "cardholder":"George Harmon",
    "number":"4111111111111111",
    "expMonth":5,
    "expYear":2012,
    "cvv":"123",
    "billingPostalCode":"60607"
  }
}

Response: 
{
  "cardholder":"George Harmon",
  "number":"************1111",
  "expMonth":5,
  "expYear":2012,
  "billingPostalCode":"60607"
}


/accounts/billing/delete - post (Remove billing info)

Request: 
{
  "email":"g@h.com",
  "password":"abc123"
}

Response: 
{
  "cardholder":"George Harmon",
  "number":"************1111",
  "expMonth":5,
  "expYear":2012,
  "billingPostalCode":"60607"
}
