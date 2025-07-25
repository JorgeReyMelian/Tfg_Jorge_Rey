import pandas as pd
import json

query1 = """ CREATE TEMP TABLE temp_user_ids AS SELECT DISTINCT user_pseudo_id FROM datatg.eventos_notnull WHERE event_name IN ('purchase', 'no_availability', 'refund', 'cancel_booking_attempt', 'cancel_booking_error', 'add_to_cart');"""

query2 = """SELECT e.user_pseudo_id, e.event_name, e.event_params, e.device, e.geo, e.event_date, e.event_timestamp, e.user_first_touch_timestamp, e.items FROM datatg.eventos_notnull e INNER JOIN temp_user_ids t ON e.user_pseudo_id = t.user_pseudo_id;"""

hotel_mapping = {
        'booking.lasgaviotas.es': 'Apartments Las Gaviotas',
        'booking.hotelfataga.com': 'Hotel LIVVO Fataga',
        'booking.lummhotel.com': 'Hotel LIVVO Lumm',
        'booking.livvohotels.com': 'Other',
        'booking.oasislanzarote.com': 'Apartamentos LIVVO Oasis',
        'booking.hotelvolcanlanzarote.com': 'Hotel LIVVO Volcán Lanzarote',
        'booking.hotelkoalagarden.com': 'Hotel LIVVO Koala Garden',
        'booking.hotelpuertodemogan.com': 'Hotel LIVVO Puerto de Mogán',
        'booking.anamarsuites.com': 'Hotel LIVVO Anamar Suites',
        'booking.miradorpapagayo.com': 'Hotel Mirador Papagayo by LIVVO',
        'booking.verilplaya.com': 'LIVVO Veril Playa Hotel',
        'booking.corralejobeach.com': 'Hotel LIVVO Corralejo Beach',
        'booking.hotelloscalderones.com': 'LIVVO Los Calderones Hotel',
        'booking.morromar.com': 'Apartamentos LIVVO Morromar',
        'booking.esmeraldaresorts.com': 'Other'
    }

# Función para parsear los parámetros del evento
def parse_event_params(event_params):
    json_data = json.loads(event_params)
    parsed_data = {}
    for entry in json_data["v"]:
        key = entry["v"]["f"][0]["v"]
        values = entry["v"]["f"][1]["v"]["f"]
        parsed_values = [v.get("v") for v in values if v.get("v") is not None]
        parsed_data[key] = parsed_values if parsed_values else None
    return parsed_data

# Función para aplicar el parseo a los eventos de compra
def apply_event_param_parsing(data_purchases):
    df_parsed = pd.DataFrame(data_purchases["event_params"].apply(parse_event_params).tolist(), index=data_purchases.index)
    for col in ['event_name', 'user_pseudo_id', 'event_date', 'items', 'time_difference']:
        df_parsed[col] = data_purchases[col]
    return df_parsed