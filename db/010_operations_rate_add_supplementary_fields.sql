create or replace view public.bank_credit_card_operations_rate_metrics as
select
    counts.institution_code,
    counts.institution_name,
    counts.period_month,
    counts.total_active_cards,
    counts.active_cards_primary,
    counts.active_cards_supplementary,
    counts.total_cards_with_operations,
    counts.operations_rate
from public.bank_credit_card_counts_curated as counts
where counts.dataset_code = 'bank_credit_card_counts';
