CREATE VIEW public.shipping_datamart AS
SELECT
s.shippingid,
s.vendorid,
st.transfer_type,
date_part('day', shipping_end_fact_datetime - shipping_start_fact_datetime) AS full_day_at_shipping,
(case WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN 1 ELSE 0 end) AS is_delay,
(case WHEN status = 'booked' THEN 1 ELSE 0 end) AS is_shipping_finish,
(case WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN date_part('day', shipping_end_fact_datetime - shipping_plan_datetime) ELSE 0 end) AS delay_day_at_shipping,
s.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) AS vat,
s.payment_amount * sa.agreement_commission AS profit
FROM
shipping AS s
JOIN public.shipping_status AS si ON s.shippingid = si.shipping_id
JOIN public.shipping_country_rates AS scr ON s.shipping_country = scr.shipping_country
JOIN public.shipping_agreement AS sa ON cast(regexp_split_to_array(s.vendor_agreement_description, E':')[1] as TEXT)  = sa.agreement_id
JOIN public.shipping_transfer AS st ON cast(regexp_split_to_array(s.shipping_transfer_description, E':') as TEXT) = st.id
