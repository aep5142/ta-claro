begin;

delete from public.cmf_dataset_sync_state
where dataset_code in (
    'bank_credit_card_purchase_volume',
    'bank_credit_card_transaction_count'
);

delete from public.cmf_datasets
where dataset_code in (
    'bank_credit_card_purchase_volume',
    'bank_credit_card_transaction_count'
);

commit;
