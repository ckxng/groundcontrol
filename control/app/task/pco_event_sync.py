import logging
from control.task_return import TR_OK, TR_GENERAL_ERROR
from control.tank import Tank
from os import getenv
from pypco import PCO
from json import dumps


class MyTank(Tank):
    def __init__(self):
        super().__init__()

        with self._conn.cursor() as cur:
            cur.execute("""
                        select exists(
                            select * 
                            from information_schema.tables 
                            where table_name='pco_event_sync'
                        )
            """)
            if not cur.fetchone()[0]:
                logging.debug("task pco_event_sync table does not exist, creating")
                cur.execute("""
                            create table pco_event_sync(
                                ds date not null default current_date, 
                                event_id serial primary key,
                                pco_id varchar(128),
                                approval_status varchar(128),
                                created_at timestamp,
                                updated_at timestamp,
                                description varchar(10240),
                                image_url varchar(1024),
                                name varchar(1024),
                                percent_approved int,
                                percent_rejected int,
                                registration_url varchar(1024),
                                summary varchar(10240),
                                visible_in_church_center bool,
                                related_owner_id varchar(128),
                                data jsonb default null,
                                timestamp timestamp not null default now()
                            )
                """)
                self._conn.commit()
            else:
                logging.debug("task pco_event_sync table exists")

    def save_event(self, event):
        with self._conn.cursor() as cur:
            logging.debug(f"task pco_event_sync MyTank save_event saving {event['id']}")
            related_owner_id = None
            if event["relationships"]["owner"]["data"] is not None:
                related_owner_id = event["relationships"]["owner"]["data"]["id"]
            cur.execute("""
                        insert into pco_event_sync (
                            pco_id,
                            approval_status,
                            created_at,
                            updated_at,
                            description,
                            image_url,
                            name,
                            percent_approved,
                            percent_rejected,
                            registration_url,
                            summary,
                            visible_in_church_center,
                            related_owner_id,
                            data
                        ) values (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
            """, (
                event["id"],
                event["attributes"].get("approval_status", None),
                event["attributes"].get("created_at", None),
                event["attributes"].get("updated_at", None),
                event["attributes"].get("description", None),
                event["attributes"].get("image_url", None),
                event["attributes"].get("name", None),
                event["attributes"].get("percent_approved", None),
                event["attributes"].get("percent_rejected", None),
                event["attributes"].get("registration_url", None),
                event["attributes"].get("summary", None),
                event["attributes"].get("visible_in_church_center", None),
                related_owner_id,
                dumps(event),
            ))
            self._conn.commit()


def run():
    logging.debug("task pco_event_sync run executing")
    events = []

    try:
        tank = MyTank()
        pco = PCO(getenv("PCO_APP_ID"), getenv("PCO_APP_SECRET"))

        for event in pco.iterate('/calendar/v2/events'):
            events.append({
                "pco_id": event['data']['id'],
            })
            logging.debug(f"task pco_event_sync found {event['data']['id']} ")
            tank.save_event(event["data"])

    except Exception as e:
        logging.critical(f"task pco_event_sync unknown error: {e}")
        return TR_GENERAL_ERROR, {
            "exception": e,
        }

    return TR_OK, {
        "all_events": events,
    }
