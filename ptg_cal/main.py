import datetime
import pprint
import itertools
import os
import re
import requests
import codecs

from contextlib import closing
from collections import namedtuple
from collections import defaultdict
from datetime import datetime as DateTime
from ics import Calendar, Event

import csv


UTC = datetime.timezone.utc
TimeSlot = namedtuple("TimeSlot", ["day","time", "sessions"])
Session = namedtuple("Session", ["start_time", "end_time", "location", "title", "tags"])
PP = pprint.PrettyPrinter(indent=4)
pprint = PP.pprint


def skip_blank_rows(reader):
    row = next(reader)
    while not any(row):
        row = next(reader)
    return row


def data_rows(reader):
    """yields only rows with events"""
    for row in reader:
        if not any(row[2:]):
            continue
        yield row


def populate_day(data):
    current_day = None
    for row in data:
        assert row[0] != "" or current_day != ""
        day = row[0] or current_day
        row[0] = day
        current_day = day
        yield row


def skip_empty_sessions(sessions):
    for session in sessions:
        if session[1]:
            yield session


def populate_session_tags(sessions):
    for session in sessions:
        tags = set()
        title = session.title.lower()
        if "cross-project" in title:
            tags.add("cross-project")
            projects = title.split(':')[1]
            tags.update(projects.split())
        elif ":" in title:
            project = title.split(':')[0]
            tags.add(project)
        elif "sig" in title:
            tags.add("sig")
        session.tags.update(tags)
        yield session


def extract_time_slots(raw_data):
    slots = []
    header_row = skip_blank_rows(raw_data)
    field_names = header_row
    field_names[0] = 'Date'
    field_names[1] = 'Time'
    data = populate_day(raw_data)
    for row in data_rows(data):
        day = row[0]
        day_of_month = int(day.split()[2])
        time_range = row[1]
        times = time_range.replace("UTC", "").split('-')
        start_time = DateTime(2020, 6, day_of_month, hour=int(times[0]), tzinfo=UTC).isoformat()
        end_time = DateTime(2020, 6, day_of_month, hour=int(times[1]), tzinfo=UTC).isoformat()
        if end_time < start_time and int(times[1])==0:
            end_time = DateTime(2020, 6, day_of_month+1, hour=int(times[1]), tzinfo=UTC).isoformat()
        raw_sessions = iter(zip(header_row[2:], row[2:]))
        filled_sessions = skip_empty_sessions(raw_sessions)
        sessions = iter(Session(start_time, end_time, *session, set()) for session in filled_sessions)
        tagged_sessions = populate_session_tags(sessions)
        slots.append(TimeSlot(day, time_range, list(tagged_sessions)))
    return slots


def construct_tag_session_mapping(sessions_by_location):
    sessions_by_tag = defaultdict(list)
    for session in sessions_by_location:
        for tag in session.tags:
            sessions_by_tag[tag].append(session)
    return sessions_by_tag


def construct_location_session_mapping(time_slots):
    sessions_by_location = defaultdict(list)
    for slot in time_slots:
        for session in slot.sessions:
            loc = session.location
            sessions_by_location[loc].append(session)
    return sessions_by_location


def merge_adjacent_sessions(sessions_by_location):
    result = {}
    for location, sessions in sessions_by_location.items():
        sessions_by_start = sorted(sessions, key=lambda x: x.start_time)
        merged_sessions = []
        session_index = 0
        previous_session = sessions_by_start[session_index]
        sessions_to_merge = []
        sessions_lenght = len(sessions_by_start)
        while session_index < sessions_lenght:
            finished = False
            peak_index = session_index
            while peak_index < sessions_lenght and not finished:
                peak_index += 1
                if peak_index == sessions_lenght:
                    finished = True
                    session_index += 1
                    if not sessions_to_merge:
                        merged_sessions.append(previous_session)
                    else:
                        session_index = peak_index
                    continue
                next_session = sessions_by_start[peak_index]
                if(next_session.title == previous_session.title
                        and next_session.start_time == previous_session.end_time):
                    # its a set dont care if i add duplicates
                    if previous_session not in sessions_to_merge:
                        sessions_to_merge.append(previous_session)
                    sessions_to_merge.append(next_session)
            if sessions_to_merge:
                start_time = sessions_to_merge[0].start_time
                end_time = sessions_to_merge[-1].end_time
                title = sessions_to_merge[0].title
                tags = sessions_to_merge[0].tags
                new_session = Session(start_time, end_time,location, title, tags)
                merged_sessions.append(new_session)
        result[location] = merged_sessions
    return result


def create_ical_folders(names, type):
    if not os.path.exists(type):
        os.mkdir(type)
    for name in names:
        path = os.path.join(type, name)
        if not os.path.exists(path):
            os.mkdir(path)


def create_ical_event_from_session(session):
    e = Event()
    e.name = session.title
    e.begin = session.start_time
    e.end = session.end_time
    e.location = session.location
    return e


def create_ical_file_per_session(session_mapping, type):
    for key, sessions in session_mapping.items():
        for session in sessions:
            title = re.sub('[^0-9A-Za-z]+', '_', session.title)
            path = os.path.join(type, key, f"{title}.ical")
            event = create_ical_event_from_session(session)
            cal = Calendar()
            cal.events.add(event)
            with open(path, 'w') as cal_file:
                cal_file.writelines(cal)


def create_ical_file_per_topic(session_mapping, type):
    for key, sessions in session_mapping.items():
        cal = Calendar()
        topic = re.sub('[^0-9A-Za-z]+', '_', key)
        path = os.path.join(type, f"{topic}.ical")
        for session in sessions:
            event = create_ical_event_from_session(session)
            cal.events.add(event)
        with open(path, 'w') as cal_file:
            cal_file.writelines(cal)


def main():
    url = "https://ethercalc.openstack.org/126u8ek25noy.csv"

    with closing(requests.get(url, stream=True)) as r:
        raw_data = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'), dialect='excel')
    # with open('data.csv') as csvfile:
    #     raw_data = csv.reader(csvfile, dialect='excel')
        slots = extract_time_slots(raw_data)
        sessions_by_location = construct_location_session_mapping(slots)
        sessions_by_location = merge_adjacent_sessions(sessions_by_location)
        merged_sessions = itertools.chain(*sessions_by_location.values())
        sessions_by_tag = construct_tag_session_mapping(merged_sessions)

        # pprint(sessions_by_tag)

        create_ical_folders(sessions_by_tag.keys(), "tags")
        create_ical_file_per_session(sessions_by_tag, "tags")
        create_ical_file_per_topic(sessions_by_tag, "tags")

        create_ical_folders(sessions_by_location.keys(), "locations")
        create_ical_file_per_session(sessions_by_location, "locations")
        create_ical_file_per_topic(sessions_by_location, "locations")

main()
