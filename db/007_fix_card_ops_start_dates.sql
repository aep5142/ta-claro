update public.cmf_datasets
set start_date = date '2009-04-01',
    updated_at = now()
where dataset_code in (
    'bank_credit_card_ops_compras_transaction_count',
    'bank_credit_card_ops_compras_nominal_volume',
    'bank_credit_card_ops_avance_en_efectivo_transaction_count',
    'bank_credit_card_ops_avance_en_efectivo_nominal_volume',
    'bank_credit_card_ops_cargos_por_servicio_transaction_count',
    'bank_credit_card_ops_cargos_por_servicio_nominal_volume'
);
