with AIQuery (
      ID
    , DELETED
    , CREATED_BY
    , DATE_ENTERED
    , MODIFIED_USER_ID
    , DATE_MODIFIED
    , DATE_MODIFIED_UTC
    , ASSIGNED_USER_ID
    , TEAM_ID
    , SALUTATION
    , FIRST_NAME
    , LAST_NAME
    , LEAD_SOURCE
    , TITLE
    , DEPARTMENT
    , BIRTHDATE
    , DO_NOT_CALL
    , PHONE_HOME
    , PHONE_MOBILE
    , PHONE_WORK
    , PHONE_OTHER
    , PHONE_FAX
    , EMAIL1
    , EMAIL2
    , ASSISTANT
    , ASSISTANT_PHONE
    , EMAIL_OPT_OUT
    , INVALID_EMAIL
    , SMS_OPT_IN
    , TWITTER_SCREEN_NAME
    , PRIMARY_ADDRESS_STREET
    , PRIMARY_ADDRESS_CITY
    , PRIMARY_ADDRESS_STATE
    , PRIMARY_ADDRESS_POSTALCODE
    , PRIMARY_ADDRESS_COUNTRY
    , ALT_ADDRESS_STREET
    , ALT_ADDRESS_CITY
    , ALT_ADDRESS_STATE
    , ALT_ADDRESS_POSTALCODE
    , ALT_ADDRESS_COUNTRY
    , DESCRIPTION
    , CAMPAIGN_ID
    , TEAM_SET_ID
    , ASSIGNED_SET_ID
    , PICTURE
    , DP_BUSINESS_PURPOSE
    , DP_CONSENT_LAST_UPDATED
    , GOOD
    , DAYS_TO_INVOICE
    , EMAILS
    )
    as
         (
             select CONTACTS.ID
                  , CONTACTS.DELETED
                  , CONTACTS.CREATED_BY
                  , CONTACTS.DATE_ENTERED
                  , CONTACTS.MODIFIED_USER_ID
                  , CONTACTS.DATE_MODIFIED
                  , CONTACTS.DATE_MODIFIED_UTC
                  , CONTACTS.ASSIGNED_USER_ID
                  , CONTACTS.TEAM_ID
                  , CONTACTS.SALUTATION
                  , CONTACTS.FIRST_NAME
                  , CONTACTS.LAST_NAME
                  , CONTACTS.LEAD_SOURCE
                  , CONTACTS.TITLE
                  , CONTACTS.DEPARTMENT
                  , CONTACTS.BIRTHDATE
                  , CONTACTS.DO_NOT_CALL
                  , CONTACTS.PHONE_HOME
                  , CONTACTS.PHONE_MOBILE
                  , CONTACTS.PHONE_WORK
                  , CONTACTS.PHONE_OTHER
                  , CONTACTS.PHONE_FAX
                  , CONTACTS.EMAIL1
                  , CONTACTS.EMAIL2
                  , CONTACTS.ASSISTANT
                  , CONTACTS.ASSISTANT_PHONE
                  , CONTACTS.EMAIL_OPT_OUT
                  , CONTACTS.INVALID_EMAIL
                  , CONTACTS.SMS_OPT_IN
                  , CONTACTS.TWITTER_SCREEN_NAME
                  , CONTACTS.PRIMARY_ADDRESS_STREET
                  , CONTACTS.PRIMARY_ADDRESS_CITY
                  , CONTACTS.PRIMARY_ADDRESS_STATE
                  , CONTACTS.PRIMARY_ADDRESS_POSTALCODE
                  , CONTACTS.PRIMARY_ADDRESS_COUNTRY
                  , CONTACTS.ALT_ADDRESS_STREET
                  , CONTACTS.ALT_ADDRESS_CITY
                  , CONTACTS.ALT_ADDRESS_STATE
                  , CONTACTS.ALT_ADDRESS_POSTALCODE
                  , CONTACTS.ALT_ADDRESS_COUNTRY
                  , CONTACTS.DESCRIPTION
                  , CONTACTS.CAMPAIGN_ID
                  , CONTACTS.TEAM_SET_ID
                  , CONTACTS.ASSIGNED_SET_ID
                  , CONTACTS.PICTURE
                  , CONTACTS.DP_BUSINESS_PURPOSE
                  , CONTACTS.DP_CONSENT_LAST_UPDATED
                  , cast(case when INVOICES.TOTAL_USDOLLAR > 0 then 1 else 0 end as bit) as GOOD
                  , (select datediff(day, CONTACTS.DATE_ENTERED, min(INVOICES.DATE_ENTERED))
                     from INVOICES
                     where INVOICES.ID = INVOICES_ACCOUNTS.INVOICE_ID
                     group by INVOICES.ID)                                    as DAYS_TO_INVOICE
                  , (select count(*)
                     from EMAILS
                     where DELETED = 0
                       and PARENT_ID in (CONTACTS.ID, ACCOUNTS.ID)
                       and TYPE in ('inbound', 'archived'))                   as EMAILS
             from CONTACTS
                      left outer join ACCOUNTS_CONTACTS
                                      on ACCOUNTS_CONTACTS.CONTACT_ID = CONTACTS.ID
                                          and ACCOUNTS_CONTACTS.DELETED = 0
                      left outer join ACCOUNTS
                                      on ACCOUNTS.ID = ACCOUNTS_CONTACTS.ACCOUNT_ID
                                          and ACCOUNTS.DELETED = 0
                      left outer join INVOICES_ACCOUNTS
                                      on INVOICES_ACCOUNTS.ACCOUNT_ID = ACCOUNTS.ID
                                          and INVOICES_ACCOUNTS.DELETED = 0
                      left outer join INVOICES
                                      on INVOICES.ID = INVOICES_ACCOUNTS.INVOICE_ID
                                          and INVOICES.DELETED = 0
             where CONTACTS.DELETED = 0
             union all
             select LEADS.ID
                  , LEADS.DELETED
                  , LEADS.CREATED_BY
                  , LEADS.DATE_ENTERED
                  , LEADS.MODIFIED_USER_ID
                  , LEADS.DATE_MODIFIED
                  , LEADS.DATE_MODIFIED_UTC
                  , LEADS.ASSIGNED_USER_ID
                  , LEADS.TEAM_ID
                  , LEADS.SALUTATION
                  , LEADS.FIRST_NAME
                  , LEADS.LAST_NAME
                  , LEADS.LEAD_SOURCE
                  , LEADS.TITLE
                  , LEADS.DEPARTMENT
                  , LEADS.BIRTHDATE
                  , LEADS.DO_NOT_CALL
                  , LEADS.PHONE_HOME
                  , LEADS.PHONE_MOBILE
                  , LEADS.PHONE_WORK
                  , LEADS.PHONE_OTHER
                  , LEADS.PHONE_FAX
                  , LEADS.EMAIL1
                  , LEADS.EMAIL2
                  , LEADS.ASSISTANT
                  , LEADS.ASSISTANT_PHONE
                  , LEADS.EMAIL_OPT_OUT
                  , LEADS.INVALID_EMAIL
                  , LEADS.SMS_OPT_IN
                  , LEADS.TWITTER_SCREEN_NAME
                  , LEADS.PRIMARY_ADDRESS_STREET
                  , LEADS.PRIMARY_ADDRESS_CITY
                  , LEADS.PRIMARY_ADDRESS_STATE
                  , LEADS.PRIMARY_ADDRESS_POSTALCODE
                  , LEADS.PRIMARY_ADDRESS_COUNTRY
                  , LEADS.ALT_ADDRESS_STREET
                  , LEADS.ALT_ADDRESS_CITY
                  , LEADS.ALT_ADDRESS_STATE
                  , LEADS.ALT_ADDRESS_POSTALCODE
                  , LEADS.ALT_ADDRESS_COUNTRY
                  , LEADS.DESCRIPTION
                  , LEADS.CAMPAIGN_ID
                  , LEADS.TEAM_SET_ID
                  , LEADS.ASSIGNED_SET_ID
                  , LEADS.PICTURE
                  , LEADS.DP_BUSINESS_PURPOSE
                  , LEADS.DP_CONSENT_LAST_UPDATED
                  , cast(case when INVOICES.TOTAL_USDOLLAR > 0 then 1 else 0 end as bit) as GOOD
                  , (select datediff(day, LEADS.DATE_ENTERED, min(INVOICES.DATE_ENTERED))
                     from INVOICES
                     where INVOICES.ID = INVOICES_ACCOUNTS.INVOICE_ID
                     group by INVOICES.ID)                                    as DAYS_TO_INVOICE
                  , (select count(*)
                     from EMAILS
                     where DELETED = 0
                       and PARENT_ID in (CONTACTS.ID, LEADS.ID, ACCOUNTS.ID)
                       and TYPE in ('inbound', 'archived'))                   as EMAILS
             from LEADS
                      left outer join CONTACTS
                                      on CONTACTS.ID = LEADS.CONTACT_ID
                                          and CONTACTS.DELETED = 0
                      left outer join ACCOUNTS_CONTACTS
                                      on ACCOUNTS_CONTACTS.CONTACT_ID = CONTACTS.ID
                                          and ACCOUNTS_CONTACTS.DELETED = 0
                      left outer join ACCOUNTS
                                      on ACCOUNTS.ID = ACCOUNTS_CONTACTS.ACCOUNT_ID
                                          and ACCOUNTS.DELETED = 0
                      left outer join INVOICES_ACCOUNTS
                                      on INVOICES_ACCOUNTS.ACCOUNT_ID = ACCOUNTS.ID
                                          and INVOICES_ACCOUNTS.DELETED = 0
                      left outer join INVOICES
                                      on INVOICES.ID = INVOICES_ACCOUNTS.INVOICE_ID
                                          and INVOICES.DELETED = 0
             where LEADS.DELETED = 0
             union all
             select PROSPECTS.ID
                  , PROSPECTS.DELETED
                  , PROSPECTS.CREATED_BY
                  , PROSPECTS.DATE_ENTERED
                  , PROSPECTS.MODIFIED_USER_ID
                  , PROSPECTS.DATE_MODIFIED
                  , PROSPECTS.DATE_MODIFIED_UTC
                  , PROSPECTS.ASSIGNED_USER_ID
                  , PROSPECTS.TEAM_ID
                  , PROSPECTS.SALUTATION
                  , PROSPECTS.FIRST_NAME
                  , PROSPECTS.LAST_NAME
                  , PROSPECTS.LEAD_SOURCE
                  , PROSPECTS.TITLE
                  , PROSPECTS.DEPARTMENT
                  , PROSPECTS.BIRTHDATE
                  , PROSPECTS.DO_NOT_CALL
                  , PROSPECTS.PHONE_HOME
                  , PROSPECTS.PHONE_MOBILE
                  , PROSPECTS.PHONE_WORK
                  , PROSPECTS.PHONE_OTHER
                  , PROSPECTS.PHONE_FAX
                  , PROSPECTS.EMAIL1
                  , PROSPECTS.EMAIL2
                  , PROSPECTS.ASSISTANT
                  , PROSPECTS.ASSISTANT_PHONE
                  , PROSPECTS.EMAIL_OPT_OUT
                  , PROSPECTS.INVALID_EMAIL
                  , PROSPECTS.SMS_OPT_IN
                  , PROSPECTS.TWITTER_SCREEN_NAME
                  , PROSPECTS.PRIMARY_ADDRESS_STREET
                  , PROSPECTS.PRIMARY_ADDRESS_CITY
                  , PROSPECTS.PRIMARY_ADDRESS_STATE
                  , PROSPECTS.PRIMARY_ADDRESS_POSTALCODE
                  , PROSPECTS.PRIMARY_ADDRESS_COUNTRY
                  , PROSPECTS.ALT_ADDRESS_STREET
                  , PROSPECTS.ALT_ADDRESS_CITY
                  , PROSPECTS.ALT_ADDRESS_STATE
                  , PROSPECTS.ALT_ADDRESS_POSTALCODE
                  , PROSPECTS.ALT_ADDRESS_COUNTRY
                  , PROSPECTS.DESCRIPTION
                  , PROSPECTS.CAMPAIGN_ID
                  , PROSPECTS.TEAM_SET_ID
                  , PROSPECTS.ASSIGNED_SET_ID
                  , PROSPECTS.PICTURE
                  , PROSPECTS.DP_BUSINESS_PURPOSE
                  , PROSPECTS.DP_CONSENT_LAST_UPDATED
                  , cast(case when INVOICES.TOTAL_USDOLLAR > 0 then 1 else 0 end as bit) as GOOD
                  , (select datediff(day, PROSPECTS.DATE_ENTERED, min(INVOICES.DATE_ENTERED))
                     from INVOICES
                     where INVOICES.ID = INVOICES_ACCOUNTS.INVOICE_ID
                     group by INVOICES.ID)                                    as DAYS_TO_INVOICE
                  , (select count(*)
                     from EMAILS
                     where DELETED = 0
                       and PARENT_ID in (CONTACTS.ID, LEADS.ID, PROSPECTS.ID, ACCOUNTS.ID)
                       and TYPE in ('inbound', 'archived'))                   as EMAILS
             from PROSPECTS
                      left outer join LEADS
                                      on LEADS.ID = PROSPECTS.LEAD_ID
                                          and LEADS.DELETED = 0
                      left outer join CONTACTS
                                      on CONTACTS.ID = LEADS.CONTACT_ID
                                          and CONTACTS.DELETED = 0
                      left outer join ACCOUNTS_CONTACTS
                                      on ACCOUNTS_CONTACTS.CONTACT_ID = CONTACTS.ID
                                          and ACCOUNTS_CONTACTS.DELETED = 0
                      left outer join ACCOUNTS
                                      on ACCOUNTS.ID = ACCOUNTS_CONTACTS.ACCOUNT_ID
                                          and ACCOUNTS.DELETED = 0
                      left outer join INVOICES_ACCOUNTS
                                      on INVOICES_ACCOUNTS.ACCOUNT_ID = ACCOUNTS.ID
                                          and INVOICES_ACCOUNTS.DELETED = 0
                      left outer join INVOICES
                                      on INVOICES.ID = INVOICES_ACCOUNTS.INVOICE_ID
                                          and INVOICES.DELETED = 0
             where PROSPECTS.DELETED = 0
         )
