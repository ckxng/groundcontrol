import logging
from control.task_return import TR_OK, TR_GENERAL_ERROR, TR_TRY_AGAIN_LATER
from control.tank import Tank
from psycopg2.extras import DictCursor
from json import dumps


class MyTank(Tank):
    def __init__(self):
        super().__init__()

        with self._conn.cursor() as cur:
            cur.execute("""
                        select exists(
                            select * 
                            from information_schema.tables 
                            where table_name='pco_event_instances_enriched'
                        )
            """)
            if not cur.fetchone()[0]:
                logging.debug("task pco_event_instances_enriched table does not exist, creating")
                cur.execute("""
                            create table pco_event_instances_enriched(
                                ds date not null default current_date, 
                                
                                enriched_event_instance_id serial primary key,

                                event_instance_pco_id varchar(128),
                                all_day_event bool,
                                compact_recurrence_description varchar(128),
                                recurrence varchar(128),
                                recurrence_description varchar(1024),
                                event_instance_created_at timestamp,
                                event_instance_updated_at timestamp,
                                starts_at timestamp,
                                ends_at timestamp,
                                church_center_url varchar(1024),
                                published_starts_at varchar(128),
                                published_ends_at varchar(128),
                                
                                event_pco_id varchar(128),
                                approval_status varchar(128),
                                event_created_at timestamp,
                                event_updated_at timestamp,
                                description varchar(10240),
                                image_url varchar(1024),
                                name varchar(1024),
                                percent_approved int,
                                percent_rejected int,
                                registration_url varchar(1024),
                                summary varchar(10240),
                                visible_in_church_center bool,
                                
                                owner_pco_id varchar(128),
                                owner_name varchar(128),
                                
                                timestamp timestamp not null default now()
                            )
                """)
                self._conn.commit()
            else:
                logging.debug("task pco_event_instances_enriched table exists")

    def are_dependencies_ready(self):
        with self._conn.cursor() as cur:
            cur.execute("select count(ds) from pco_event_instance_sync where ds = current_date")
            results = cur.fetchone()
            if results is None or results[0] < 1:
                return False

            cur.execute("select count(ds) from pco_event_sync where ds = current_date")
            results = cur.fetchone()
            if results is None or results[0] < 1:
                return False

            cur.execute("select count(ds) from pco_people_sync where ds = current_date")
            results = cur.fetchone()
            if results is None or results[0] < 1:
                return False

        return True

    def create_enriched_dataset(self):
        with self._conn.cursor() as cur:
            logging.debug("task pco_event_instances_enriched MyTank create_enriched_dataset executing")

            cur.execute("""
                        insert into pco_event_instances_enriched (
                                                                  event_instance_pco_id,
                                                                  all_day_event,
                                                                  compact_recurrence_description,
                                                                  recurrence,
                                                                  recurrence_description,
                                                                  event_instance_created_at,
                                                                  event_instance_updated_at,
                                                                  starts_at,
                                                                  ends_at,
                                                                  church_center_url,
                                                                  published_starts_at,
                                                                  published_ends_at,
                                                                  event_pco_id,
                                                                  approval_status,
                                                                  event_created_at,
                                                                  event_updated_at,
                                                                  description,
                                                                  image_url,
                                                                  name,
                                                                  percent_approved,
                                                                  percent_rejected,
                                                                  registration_url,
                                                                  summary,
                                                                  visible_in_church_center,
                                                                  owner_pco_id,
                                                                  owner_name
                        )
                        select
                            i.pco_id,
                            i.all_day_event,
                            i.compact_recurrence_description,
                            i.recurrence,
                            i.recurrence_description,
                            i.created_at,
                            i.updated_at,
                            i.starts_at,
                            i.ends_at,
                            i.church_center_url,
                            i.published_starts_at,
                            i.published_ends_at,
                            e.pco_id,
                            e.approval_status,
                            e.created_at,
                            e.updated_at,
                            e.description,
                            e.image_url,
                            e.name,
                            e.percent_approved,
                            e.percent_rejected,
                            e.registration_url,
                            e.summary,
                            e.visible_in_church_center,
                            p.pco_id,
                            p.name
                        from
                            pco_event_instance_sync i
                            left join pco_event_sync e on e.pco_id = i.related_event_id
                            left join pco_people_sync p on p.pco_id = e.related_owner_id
                        where
                            i.ds = current_date
                            and e.ds = current_date
                            and p.ds = current_date
            """)
            self._conn.commit()


def run():
    logging.debug("task pco_event_instances_enriched run executing")
    events = []

    try:
        tank = MyTank()

        if not tank.are_dependencies_ready():
            return TR_TRY_AGAIN_LATER, {
                "reason": "one or more tables do not have data loaded in today's dataset"
            }

        tank.create_enriched_dataset()

    except Exception as e:
        logging.critical(f"task pco_event_instances_enriched unknown error: {e}")
        return TR_GENERAL_ERROR, {
            "exception": e,
        }

    return TR_OK, {
        "all_event_instances_enriched": events,
    }
