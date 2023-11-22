import logging
from control.task_return import TR_OK, TR_GENERAL_ERROR, TR_UPSTREAM_DATA_INVALID
from control.tank import Tank
from os import getenv
from pypco import PCO
from json import dumps


class PCOEventSyncUpstreamDataInvalidError(Exception):
    pass


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
            pco_id = None
            attributes = None
            try:
                pco_id = event["data"]["id"]
                attributes = event["data"]["attributes"]
            except KeyError as e:
                raise PCOEventSyncUpstreamDataInvalidError(
                    f"cannot save an event with missing data attributes or id: {e}")

            related_owner_id = None
            try:
                related_owner_id = int(event["data"]["relationships"]["owner"]["data"]["id"])
            except KeyError:
                pass
            except ValueError:
                pass
            except TypeError:
                pass

            logging.debug(f"task pco_event_sync MyTank save_event saving {pco_id}")
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
                pco_id,
                attributes.get("approval_status", None),
                attributes.get("created_at", None),
                attributes.get("updated_at", None),
                attributes.get("description", None),
                attributes.get("image_url", None),
                attributes.get("name", None),
                attributes.get("percent_approved", None),
                attributes.get("percent_rejected", None),
                attributes.get("registration_url", None),
                attributes.get("summary", None),
                attributes.get("visible_in_church_center", None),
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

        for event in pco.iterate('/calendar/v2/events?include=tags,feed'):
            pco_id = event.get("data", {}).get("id", "?")
            logging.debug(f"task pco_event_sync found {pco_id} ")
            events.append({
                "pco_id": pco_id,
            })
            tank.save_event(event)

    except PCOEventSyncUpstreamDataInvalidError as e:
        logging.critical(f"task pco_event_sync upstream invalid data error: {e}")
        return TR_UPSTREAM_DATA_INVALID, {
            "exception": e,
            "possible_error_in": events[-1],
            "events_processed": events[:-1],
        }

    except Exception as e:
        logging.critical(f"task pco_event_sync unknown error: {e}")
        return TR_GENERAL_ERROR, {
            "exception": e,
            "events_processed": events[:-1],
        }

    return TR_OK, {
        "all_events": events,
    }
