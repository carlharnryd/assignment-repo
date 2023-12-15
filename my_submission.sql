SELECT
  DATETIME(derived_tstamp,"Europe/Berlin") NO_time,--Norwegian time
  user_role,
  user_id,
  customers,
  session_id,
  page_url,--Not including the querystring
  is_exit_page,--Is the page an exit page
  
  --Below calculates the time either if it is an exit page or a page usual page in the session, null if the event is not a pageview event
  CASE 
    WHEN is_exit_page=1 THEN TIMESTAMP_DIFF(last_session_interaction,derived_tstamp,MINUTE)
    WHEN is_exit_page=0 THEN TIMESTAMP_DIFF(next_pageview_time,derived_tstamp,MINUTE)
  ELSE
  NULL
END
  page_time,

  page_view_count,
  link_click_count,
  submit_form_count,
  focus_form_count,
  change_form_count
FROM ((
    SELECT
      derived_tstamp,--Choosen because several sources used this as the time
      user_type user_role,--Only admin now but we take hight for that more roles might be added in the future
      user_id user_id,
      hashed_customer_name customers,--Choosen because most probably uniq
      domain_sessionid session_id,
      REGEXP_EXTRACT(hashed_page_url, r'^[^?]*') page_url,--Removes the querystring from the url
      event_id event_identifier,--Unique row identifier for the left join below
      LAST_VALUE(derived_tstamp) OVER (PARTITION BY domain_sessionid ORDER BY derived_tstamp) last_session_interaction,--Last event in session, could be click or any other event

      --Below is event counters (I should have taken into account more can come in the future)
      CASE
        WHEN event_name = 'page_view' THEN 1
      ELSE
      0
    END
      page_view_count,
      CASE
        WHEN event_name = 'link_click' THEN 1
      ELSE
      0
    END
      link_click_count,
      CASE
        WHEN event_name = 'submit_form' THEN 1
      ELSE
      0
    END
      submit_form_count,
      CASE
        WHEN event_name = 'focus_form' THEN 1
      ELSE
      0
    END
      focus_form_count,
      CASE
        WHEN event_name = 'change_form' THEN 1
      ELSE
      0
    END
      change_form_count
    FROM
      `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
    WHERE
      --Below takes all date before today
      TIMESTAMP_TRUNC(derived_tstamp, DAY) < TIMESTAMP(CURRENT_DATE()) 
      --Below removes duplicated event rows (find the solution on Snowplow)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY collector_tstamp) = 1)
      ------------------------------------------------------------------------------------
  LEFT JOIN 
  ---------------------------------------------------------------------------------------
  (SELECT
      event_id event_id,--Identifier for the left join
      LEAD(derived_tstamp) OVER (PARTITION BY domain_sessionid ORDER BY derived_tstamp) next_pageview_time,--Next pageviews time
      --Below defines bolean: is exitpage
      CASE
        WHEN (LEAD(derived_tstamp) OVER (PARTITION BY domain_sessionid ORDER BY derived_tstamp)) IS NULL THEN 1
      ELSE
      0
    END
      is_exit_page

    FROM
      `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
    WHERE
      event_name='page_view'
      --Below takes all date before today
      AND TIMESTAMP_TRUNC(derived_tstamp, DAY) < TIMESTAMP(CURRENT_DATE()) 
      --Below removes duplicated event rows (find the solution on Snowplow)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY collector_tstamp) = 1)
  ON
    event_id=event_identifier)