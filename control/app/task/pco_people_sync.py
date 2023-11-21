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
            logging.debug("task pco_people_sync MyTank save_person saving "
                          f"{person['attributes']['first_name']} {person['attributes']['last_name']}")
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
                person["id"],
                person["attributes"].get("first_name", None),
                person["attributes"].get("given_name", None),
                person["attributes"].get("nickname", None),
                person["attributes"].get("last_name", None),
                person["attributes"].get("birthdate", None),
                person["attributes"].get("anniversary", None),
                person["attributes"].get("gender", None),
                person["attributes"].get("grade", None),
                person["attributes"].get("child", None),
                person["attributes"].get("graduation_year", None),
                person["attributes"].get("site_administrator", None),
                person["attributes"].get("accounting_administrator", None),
                person["attributes"].get("people_permissions", None),
                person["attributes"].get("membership", None),
                person["attributes"].get("inactivated_at", None),
                person["attributes"].get("status", None),
                person["attributes"].get("medical_notes", None),
                person["attributes"].get("mfa_configured", None),
                person["attributes"].get("created_at", None),
                person["attributes"].get("updated_at", None),
                person["attributes"].get("avatar", None),
                person["attributes"].get("name", None),
                person["attributes"].get("demographic_avatar_url", None),
                person["attributes"].get("directory_status", None),
                person["attributes"].get("passed_background_check", None),
                person["attributes"].get("can_create_forms", None),
                person["attributes"].get("can_email_lists", None),
                person["attributes"].get("school_type", None),
                person["attributes"].get("remote_id", None),
                dumps(person),
            ))
            self._conn.commit()


def run():
    logging.debug("task pco_people_sync run executing")
    people = []

    try:
        tank = MyTank()
        pco = PCO(getenv("PCO_APP_ID"), getenv("PCO_APP_SECRET"))

        for person in pco.iterate('/people/v2/people'):
            people.append({
                "pco_id": person['data']['id'],
                "first_name": person['data']['attributes']['first_name'],
                "last_name": person['data']['attributes']['last_name'],
            })
            logging.debug("task pco_people_sync found "
                          f"{person['data']['id']} "
                          f"{person['data']['attributes']['first_name']} "
                          f"{person['data']['attributes']['last_name']}")
            tank.save_person(person["data"])

    except Exception as e:
        logging.critical(f"task pco_people_sync unknown error: {e}")
        return TR_GENERAL_ERROR, {
            "exception": e,
        }

    return TR_OK, {
        "all_people": people,
    }
