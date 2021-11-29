"""
This python script polls the specified GTFS-RT endpoints in the endpoints map for vehicle positions, service alerts, and trip updates.
It stores data in three directories (assumed created) - vehicle, alert, and trip_update 
in the format <directory>/<authority>__<data_type>_<timestamp>
e.g. vehicle/ac_transit_vehicle_1637272250
"""

from google.transit import gtfs_realtime_pb2
import csv
import time
import urllib
from urllib.request import urlopen

# replace with actual AC Transit token
AC_TRANSIT_TOKEN = 'XXXXXXXXX'

class GTFSEndpoint:
	def __init__(self, url, vehicle, alert, trip_update):
		self.url = url
		self.vehicle = vehicle
		self.alert = alert
		self.trip_update = trip_update

	def get_endpoint(self, data_type):
		return self.url + getattr(self, data_type, "")

endpoint_mapping = {
	# AC Transit - http://api.actransit.org/transit/Help
	'AC_TRANSIT': GTFSEndpoint('http://api.actransit.org/transit/', 'gtfsrt/vehicles', 'gtfsrt/alerts', 'gtfsrt/tripupdates'),
	
	# MBTA - https://www.mbta.com/developers/gtfs-realtime
	'MBTA': GTFSEndpoint('https://cdn.mbta.com/realtime/', 'VehiclePositions.pb', 'Alerts.pb', 'TripUpdates.pb'),

	# VIA San Antonio TX - https://www.viainfo.net/developers-resources/ - Not very reliable for frequent polling (I think there's a throttle)
	# 'VIA_SAN_ANTONIO': GTFSEndpoint('http://gtfs.viainfo.net/', 'vehicle/vehiclepositions.pb', 'alert/alerts.pb', 'tripupdate/tripupdates.pb')

	# Community Transit Everett WA - https://www.communitytransit.org/OpenData
	'COMM_TRANS': GTFSEndpoint('http://s3.amazonaws.com/commtrans-realtime-prod/', 'vehiclepositions.pb', 'alerts.pb', 'tripupdates.pb'),

	# CT Transit Hartford CT - https://www.cttransit.com/about/developers
	'CT_TRANSIT': GTFSEndpoint('https://s3.amazonaws.com/cttransit-realtime-prod/', 'vehiclepositions.pb', 'alerts.pb', 'tripupdates.pb')

}

vehicle_trip_mapping = {}

CAUSE = {
	0: '',
	1: 'UNKNOWN_CAUSE',
	2: 'OTHER_CAUSE', 
	3: 'TECHNICAL_PROBLEM', 
	4: 'STRIKE', 
	5: 'DEMONSTRATION', 
	6: 'ACCIDENT', 
	7: 'HOLIDAY', 
	8: 'WEATHER', 
	9: 'MAINTENANCE', 
	10: 'CONSTRUCTION', 
	11: 'POLICE_ACTIVITY', 
	12: 'MEDICAL_EMERGENCY'
}

EFFECT = {
	0: '',
	1: 'NO_SERVICE',
	2: 'REDUCED_SERVICE',
	3: 'SIGNIFICANT_DELAYS',
	4: 'DETOUR',
	5: 'ADDITIONAL_SERVICE',
	6: 'MODIFIED_SERVICE',
	7: 'OTHER_EFFECT',
	8: 'UNKNOWN_EFFECT',
	9: 'STOP_MOVED'
}

VEHICLE_STOP_STATUS = {
	0: 'INCOMING_AT',
	1: 'STOPPED_AT',
	2: 'IN_TRANSIT_TO'
}

CONGESTION_LEVEL = {
	0: 'UNKNOWN_CONGESTION_LEVEL',
	1: 'RUNNING_SMOOTHLY',
	2: 'STOP_AND_GO',
	3: 'CONGESTION',
	4: 'SEVERE_CONGESTION'
}

OCCUPANCY_STATUS = {
	0: '',
	1: 'EMPTY',
	2: 'MANY_SEATS_AVAILABLE',
	3: 'FEW_SEATS_AVAILABLE',
	4: 'STANDING_ROOM_ONLY',
	5: 'CRUSHED_STANDING_ROOM_ONLY',
	6: 'FULL',
	7: 'NOT_ACCEPTING_PASSENGERS'
}

SCHEDULE_RELATIONSHIP = {
	0: 'SCHEDULED',
	1: 'ADDED',
	2: 'UNSCHEDULED',
	3: 'CANCELED',
}

STOP_TIME_SCHEDULE_RELATIONSHIP = {
	0: 'SCHEDULED',
	1: 'SKIPPED',
	2: 'NO_DATA'
}


# Refer to https://developers.google.com/transit/gtfs-realtime/reference for details on fields
CSV_HEADERS = {
	'vehicle': [
		'id', 
		'trip.trip_id','trip.route_id','trip.direction_id','trip.start_time','trip.start_date','trip.schedule_relationship',
		'vehicle.id', 'vehicle.label', 'vehicle.license_plate',
		'position.latitude', 'position.longitude', 'position.bearing', 'position.odometer', 'position.speed',
		'current_stop_sequence',
		'stop_id',
		'current_status',
		'timestamp',
		'congestion_level',
		'occupancy_status'
	],
	'alert': [
		'id',
		'active_period',
		'informed_entity',
		'cause', 
		'effect', 
		'header_text', 
		'description_text'
	],
	'trip_update': [
		'id',
		'trip.trip_id','trip.route_id','trip.direction_id','trip.start_time','trip.start_date','trip.schedule_relationship',
		'vehicle.id', 'vehicle.label', 'vehicle.license_plate',
		'stop_time_update',
		'timestamp', 
		'delay'
	]
}

def convert_to_csv_row(entity, data_type):
	if data_type == 'vehicle':
		v = entity.vehicle
		trip_id = v.trip.trip_id
		if not v.trip.HasField('trip_id') and v.vehicle.id in vehicle_trip_mapping:
			trip_id = vehicle_trip_mapping[v.vehicle.id]
		row = [
			entity.id,
			trip_id, v.trip.route_id, v.trip.direction_id, v.trip.start_time, v.trip.start_date, SCHEDULE_RELATIONSHIP[v.trip.schedule_relationship],
			v.vehicle.id, v.vehicle.label, v.vehicle.license_plate,
			v.position.latitude, v.position.longitude, v.position.bearing, v.position.odometer, v.position.speed,
			v.current_stop_sequence,
			v.stop_id,
			VEHICLE_STOP_STATUS[v.current_status],
			v.timestamp,
			CONGESTION_LEVEL[v.congestion_level],
			OCCUPANCY_STATUS[v.occupancy_status]
		]
		return row
	elif data_type == 'alert':
		a = entity.alert
		row = [
			entity.id,
			process_many_field(a.active_period),
			process_informed_entity(a.informed_entity),
			CAUSE[a.cause],
			EFFECT[a.effect],
			process_translation(a.header_text.translation),
			process_translation(a.description_text.translation)
		]
		return row
	elif data_type == 'trip_update':
		t = entity.trip_update
		row = [
			entity.id,
			t.trip.trip_id,t.trip.route_id,t.trip.direction_id,t.trip.start_time,t.trip.start_date,SCHEDULE_RELATIONSHIP[t.trip.schedule_relationship],
			t.vehicle.id, t.vehicle.label, t.vehicle.license_plate,
			process_stop_time_update(t.stop_time_update),
			t.timestamp, 
			t.delay
		]
		if t.HasField('vehicle'):
			vehicle_trip_mapping[t.vehicle.id] = t.trip.trip_id
		return row

	return []

def process_many_field(field):
	return [str(elem).replace('\n', ' ') for elem in field]

def process_stop_time_update(stu_field):
	ret = []
	for stu in stu_field:
		stu_obj = {}
		stu_obj['stop_sequence'] = stu.stop_sequence
		stu_obj['stop_id'] = stu.stop_id
		stu_obj['arrival'] = stu.arrival.time
		stu_obj['departure'] = stu.departure.time
		stu_obj['schedule_relationship'] = STOP_TIME_SCHEDULE_RELATIONSHIP[stu.schedule_relationship]
		ret.append(stu_obj)
	return ret

def process_informed_entity(informed):
	ret = []
	for e in informed:
		e_obj = {}
		e_obj['agency_id'] = e.agency_id
		e_obj['route_id'] = e.route_id
		e_obj['stop_id'] = e.stop_id
		ret.append(e_obj)
	return ret


def process_translation(translation_field):
	for translation in translation_field:
		if translation.language == 'en':
			return translation.text
	return ''

def poll():
	timestamp = str(int(time.time()))
	for authority_name in endpoint_mapping.keys():
		endpoint_obj = endpoint_mapping.get(authority_name)
		for data_type in ['trip_update', 'vehicle', 'alert']:
			# Build endpoint and parse response
			endpoint = endpoint_obj.get_endpoint(data_type)
			if authority_name == "AC_TRANSIT":
				endpoint = endpoint + '/?token=' + AC_TRANSIT_TOKEN
			response = urllib.request.urlopen(endpoint)
			feed = gtfs_realtime_pb2.FeedMessage()
			feed.ParseFromString(response.read())

			# Write data to csv (at path vehicle/ac_transit_vehicle_1637272250, e.g.)
			# prefix = '/home/ec2-user/gtfsrt'
			prefix = ''
			file_path = prefix + data_type + '/' + authority_name.lower() + '_' + data_type + '_' + timestamp + '.csv'
			with open(file_path, 'w') as f:
				writer = csv.writer(f)
				# Write header for appropriate data type
				writer.writerow(CSV_HEADERS.get(data_type))
				# Write entities as separate rows
				for entity in feed.entity:
					writer.writerow(convert_to_csv_row(entity, data_type))


if __name__ == "__main__":
	poll()