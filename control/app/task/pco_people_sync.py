import logging
from control.task_return import TR_OK, TR_GENERAL_ERROR, TR_UPSTREAM_DATA_INVALID
from control.tank import Tank
from os import getenv
from pypco import PCO
from json import dumps


class PCOPeopleSyncUpstreamDataInvalidError(Exception):
    pass


class MyTank(Tank):
    def __init__(self):
        super().__init__()

        with self._conn.cursor() as cur:
            cur.execute("""
                        select exists(
                            select * 
                            from information_schema.tables 
                            where table_name='pco_people_sync'
                        )
            """)
            if not cur.fetchone()[0]:
                logging.debug("task pco_people_sync table does not exist, creating")
                cur.execute("""
                            create table pco_people_sync(
                                ds date not null default current_date, 
                                person_id serial primary key,
                                pco_id varchar(128),
                                first_name varchar(128),
                                given_name varchar(128),
                                nickname varchar(128),
                                last_name varchar(128),
                                birthdate date,
                                anniversary date,
                                gender varchar(128),
                                grade int,
                                child bool,
                                graduation_year int,
                                site_administrator bool,
                                accounting_administrator bool,
                                people_permissions varchar(128),
                                membership varchar(128),
                                inactivated_at timestamp,
                                status varchar(128),
                                medical_notes varchar(1024),
                                mfa_configured bool,
                                created_at timestamp,
                                updated_at timestamp,
                                avatar varchar(1024),
                                name varchar(128),
                                demographic_avatar_url varchar(1024),
                                directory_status varchar(128),
                                passed_background_check bool,
                                can_create_forms bool,
                                can_email_lists bool,
                                school_type varchar(128),
                                remote_id int,
                                data jsonb default null,
                                timestamp timestamp not null default now()
                            )
                """)
                self._conn.commit()
            else:
                logging.debug("task pco_people_sync table exists")

    def save_person(self, person):
        with self._conn.cursor() as cur:
            pco_id = None
            attributes = None
            try:
                pco_id = person["data"]["id"]
                attributes = person["data"]["attributes"]
            except KeyError as e:
                raise PCOPeopleSyncUpstreamDataInvalidError(
                    f"cannot save a person with missing data attributes or id: {e}")

            logging.debug(f"task pco_people_sync MyTank save_person saving {pco_id}")
            cur.execute("""
                        insert into pco_people_sync (
                            pco_id,
                            first_name,
                            given_name,
                            nickname,
                            last_name,
                            birthdate,
                            anniversary,
                            gender,
                            grade,
                            child,
                            graduation_year,
                            site_administrator,
                            accounting_administrator,
                            people_permissions,
                            membership,
                            inactivated_at,
                            status,
                            medical_notes,
                            mfa_configured,
                            created_at,
                            updated_at,
                            avatar,
                            name,
                            demographic_avatar_url,
                            directory_status,
                            passed_background_check,
                            can_create_forms,
                            can_email_lists,
                            school_type,
                            remote_id,
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
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
            """, (
                pco_id,
                attributes.get("first_name", None),
                attributes.get("given_name", None),
                attributes.get("nickname", None),
                attributes.get("last_name", None),
                attributes.get("birthdate", None),
                attributes.get("anniversary", None),
                attributes.get("gender", None),
                attributes.get("grade", None),
                attributes.get("child", None),
                attributes.get("graduation_year", None),
                attributes.get("site_administrator", None),
                attributes.get("accounting_administrator", None),
                attributes.get("people_permissions", None),
                attributes.get("membership", None),
                attributes.get("inactivated_at", None),
                attributes.get("status", None),
                attributes.get("medical_notes", None),
                attributes.get("mfa_configured", None),
                attributes.get("created_at", None),
                attributes.get("updated_at", None),
                attributes.get("avatar", None),
                attributes.get("name", None),
                attributes.get("demographic_avatar_url", None),
                attributes.get("directory_status", None),
                attributes.get("passed_background_check", None),
                attributes.get("can_create_forms", None),
                attributes.get("can_email_lists", None),
                attributes.get("school_type", None),
                attributes.get("remote_id", None),
                dumps(person),
            ))
            self._conn.commit()


def run():
    logging.debug("task pco_people_sync run executing")
    people = []

    try:
        tank = MyTank()
        pco = PCO(getenv("PCO_APP_ID"), getenv("PCO_APP_SECRET"))

        for person in pco.iterate("/people/v2/people?include=addresses,emails,field_data,households,"
                                  "inactive_reason,marital_status,name_prefix,name_suffix,organization,person_apps,"
                                  "phone_numbers,platform_notifications,primary_campus,school,social_profiles"):
            pco_id = person.get("data", {}).get("id", "?")
            logging.debug(f"task pco_people_sync found {pco_id}")
            people.append({
                "pco_id": pco_id,
            })
            tank.save_person(person)

    except PCOPeopleSyncUpstreamDataInvalidError as e:
        logging.critical(f"task pco_people_sync upstream invalid data error: {e}")
        return TR_UPSTREAM_DATA_INVALID, {
            "exception": e,
            "possible_error_in": people[-1],
            "people_processed": people[:-1],
        }

    except Exception as e:
        logging.critical(f"task pco_people_sync unknown error: {e}")
        return TR_GENERAL_ERROR, {
            "exception": e,
            "possible_error_in": people[-1],
            "people_processed": people[:-1],
        }

    return TR_OK, {
        "all_people": people,
    }
