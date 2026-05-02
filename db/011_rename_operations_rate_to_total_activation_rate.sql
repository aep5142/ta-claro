update public.cmf_datasets
set operation_type = 'Total Activation Rate',
    updated_at = now()
where operation_type = 'Operations Rate'
  and dataset_code in (
    'bank_credit_card_active_cards_primary',
    'bank_credit_card_active_cards_supplementary',
    'bank_credit_card_cards_with_operations_primary',
    'bank_credit_card_cards_with_operations_supplementary'
  );
