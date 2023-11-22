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
                            where table_name='pco_event_instance_sync'
                        )
            """)
            if not cur.fetchone()[0]:
                logging.debug("task pco_event_instance_sync table does not exist, creating")
                cur.execute("""
                            create table pco_event_instance_sync(
                                ds date not null default current_date, 
                                event_instance_id serial primary key,
                                pco_id varchar(128),
                                all_day_event bool,
                                compact_recurrence_description varchar(128),
                                recurrence varchar(128),
                                recurrence_description varchar(1024),
                                created_at timestamp,
                                updated_at timestamp,
                                starts_at timestamp,
                                ends_at timestamp,
                                church_center_url varchar(1024),
                                published_starts_at varchar(128),
                                published_ends_at varchar(128),
                                related_event_id varchar(128),
                                data jsonb default null,
                                timestamp timestamp not null default now()
                            )
                """)
                self._conn.commit()
            else:
                logging.debug("task pco_event_instance_sync table exists")

    def save_event_instance(self, instance):
        with self._conn.cursor() as cur:
            logging.debug(f"task pco_event_instance_sync MyTank save_event_instance saving {instance['id']}")
            cur.execute("""
                        insert into pco_event_instance_sync (
                            pco_id,
                            all_day_event,
                            compact_recurrence_description,
                            recurrence,
                            recurrence_description,
                            created_at,
                            updated_at,
                            starts_at,
                            ends_at,
                            church_center_url,
                            published_starts_at,
                            published_ends_at,
                            related_event_id,
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
                instance["id"],
                instance["attributes"].get("all_day_event", None),
                instance["attributes"].get("compact_recurrence_description", None),
                instance["attributes"].get("recurrence", None),
                instance["attributes"].get("recurrence_description", None),
                instance["attributes"].get("created_at", None),
                instance["attributes"].get("updated_at", None),
                instance["attributes"].get("starts_at", None),
                instance["attributes"].get("ends_at", None),
                instance["attributes"].get("church_center_url", None),
                instance["attributes"].get("published_starts_at", None),
                instance["attributes"].get("published_ends_at", None),
                instance["relationships"]["event"]["data"]["id"],
                dumps(instance),
            ))
            self._conn.commit()


def run():
    logging.debug("task pco_event_instance_sync run executing")
    instances = []

    try:
        tank = MyTank()
        pco = PCO(getenv("PCO_APP_ID"), getenv("PCO_APP_SECRET"))

        for instance in pco.iterate('/calendar/v2/event_instances'):
            instances.append({
                "pco_id": instance['data']['id'],
            })
            logging.debug(f"task pco_event_instance_sync found {instance['data']['id']} ")
            tank.save_event_instance(instance["data"])

    except Exception as e:
        logging.critical(f"task pco_event_instance_sync unknown error: {e}")
        return TR_GENERAL_ERROR, {
            "exception": e,
        }

    return TR_OK, {
        "all_event_instances": instances,
    }
