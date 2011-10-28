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

Account Manipulation
====================

ReportGrid'si account API is located at:

http://api.reportgrid.com/services/billing/v1/accounts/

Account Creation
----------------

Tokens can be created by POSTing to the ReportGrid tokens API with a JSON object that describes the path, permissions, 
and limits of the token.  A descendent token's permissions and limits cannot exceed (but may equal) the parent's. 

+--------------------+------------------------------------------------------------------------+
| method             | PUT                                                                    |
+--------------------+------------------------------------------------------------------------+
| url pattern        | (API ROOT)/accounts/                                                   |
+--------------------+------------------------------------------------------------------------+
| body               | A JSON object describing the properties of the account                 |
|                    | See below for an example.                                              |
+--------------------+------------------------------------------------------------------------+
| request parameters | none                                                                   | 
+--------------------+------------------------------------------------------------------------+

:: Request

  {
    "email":"g@h.com",
    "password":"abc123",
    "planId":"bronze",
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
    "billing":{
      "cardholder":"George Harmon",
      "number":"4111111111111111",
      "expMonth":5,
      "expYear":2012,
      "cvv":"123"
    }
  }

+--------------------+------------------------------------------------------------------------------------------+
| email              | unique user email address (duplicate email addresses will be rejected)                   | 
+--------------------+------------------------------------------------------------------------------------------+
| password           | account password                                                                         | 
+--------------------+------------------------------------------------------------------------------------------+
| planId             | id for the given service plan options: {starter,bronze,silver,gold}                      | 
+--------------------+------------+-----------------------------------------------------------------------------+
| contact            | firstName  |                                                                             |
|                    +------------+-----------------------------------------------------------------------------+
|                    | lastName   |                                                                             |
|                    +------------+-----------------------------------------------------------------------------+
|                    | company    |                                                                             |
|                    +------------+-----------------------------------------------------------------------------+
|                    | title      |                                                                             |
|                    +------------+-----------------------------------------------------------------------------+
|                    | phone      |                                                                             |
|                    +------------+-----------------------------------------------------------------------------+
|                    | website    |                                                                             |
|                    +------------+------------+----------------------------------------------------------------+
|                    | address    | street     |                                                                |
|                    |            +------------+----------------------------------------------------------------+
|                    |            | city       |                                                                |
|                    |            +------------+----------------------------------------------------------------+
|                    |            | state      |                                                                |
|                    |            +------------+----------------------------------------------------------------+
|                    |            | postalCode |                                                                |
+--------------------+------------+-----------------------------------------------------------------------------+
| billing            | cardholder | name on credit card                                                         |
|                    +------------+-----------------------------------------------------------------------------+
|                    | number     | credit card number                                                          |
|                    +------------+-----------------------------------------------------------------------------+
|                    | expMonth   | card expiration month                                                       |
|                    +------------+-----------------------------------------------------------------------------+
|                    | expYear    | card expiration year                                                        |
|                    +------------+-----------------------------------------------------------------------------+
|                    | cvv        | card security code                                                          |
+--------------------+------------+-----------------------------------------------------------------------------+

:: Response

  {
    "id":{
      "token":"EEC524BA-9116-4725-9F33-5ECB0493D31C",
      "email":"a@b.com"
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
      "accountCreated":1319753105174,
      "credit":25000,
      "lastCreditAssessment":1319673600000,
      "usage":0,
      "status":"ACTIVE",
      "gracePeriodExpires":null,
      "subscriptionId":null
    },
    "billing":{
      "cardholder":"George Harmon",
      "number":"************1111",
      "expMonth":5,
      "expYear":2012
    }
  }
 

Get Account
-----------

To retrieve account information given the account email or token and the account password.

+--------------------+------------------------------------------------------------------------+
| method             | POST                                                                   |
+--------------------+------------------------------------------------------------------------+
| url pattern        | (API ROOT)/accounts/get                                                |
+--------------------+------------------------------------------------------------------------+
| request parameters |                                                                        |
+--------------------+------------------------------------------------------------------------+

:: Request

  {
    "accountToken":"ACF64717-868A-459B-84CE-90FDC70A2D2B",
    "password":"abc123"
  }

  or

  {
    "email":"notIn@accounts.com",
    "password":"abc123"
  }

  or (token takes precedence and email will be ignored)

  {
    "accountToken":"ACF64717-868A-459B-84CE-90FDC70A2D2B",
    "email":"notIn@accounts.com",
    "password":"abc123"
  }

:: Response

{ same as create account }
 
Accounts Assessment (Internal)
------------------------------

Runs daily account assessment (can be run multiple times without ill affect and can also be run
after more than one days passed although subscriptions will be actived on the day of the run not
retroactively for accounts with expired credits between runs.)

+--------------------+------------------------------------------------------------------------+
| method             | POST                                                                   |
+--------------------+------------------------------------------------------------------------+
| url pattern        | (API ROOT)/accounts/assess                                             |
+--------------------+------------------------------------------------------------------------+
| request parameters |                                                                        |
+--------------------+------------------------------------------------------------------------+
 
:: Request

<< no request body - considering a 'secret token' as parameter or payload just to be defensive >>

:: Response

<< currently no response - anticipate assessment result summary >>
