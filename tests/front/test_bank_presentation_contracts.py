from pathlib import Path


def test_bank_presentation_uses_curated_corporate_palette_with_fallback():
    src = Path("front/lib/bank-presentation.ts").read_text()

    assert '"SMU Corp S.A.": "Unipay"' in src
    assert '"Banco del Estado de Chile": "BancoEstado"' in src
    assert '"Banco Bice": "BICE"' in src
    assert '"Banco BICE": "BICE"' in src
    assert '"Banco París": "Paris"' in src
    assert '"Banco Itaú Chile": "Itaú"' in src
    assert '"Banco Falabella": "Falabella"' in src
    assert '"Banco Internacional": "Internacional"' in src
    assert '"Banco Santander": "Santander"' in src
    assert '"Banco Security": "Security"' in src
    assert '"Los Héroes prepago": "Los Héroes"' in src
    assert '"Global Card": "Global 66"' in src
    assert '"Inversiones LP": "La Polar"' in src
    assert 'SUMUP: "Sumup"' in src
    assert '"Los Andes prepago": "Los Andes"' in src
    assert 'PREX: "Prex"' in src
    assert '"Mercado pago": "Mercado Pago"' in src
    assert "const CORPORATE_BANK_COLORS: Record<string, string> = {" in src
    assert '"Líder BCI": "#0053E1"' in src
    assert 'Scotiabank: "#EC111A"' in src
    assert 'Unipay: "#683DF2"' in src
    assert 'BancoEstado: "#FF7900"' in src
    assert '"Banco Estado": "#FF7900"' in src
    assert 'Consorcio: "#2F6D9B"' in src
    assert '"Banco de Chile": "#3A7BFF"' in src
    assert 'Paris: "#006DFF"' in src
    assert '"Cencosud Scotiabank": "#4B7A98"' in src
    assert '"Banco Ripley": "#523178"' in src
    assert 'CMF_RIPLEY: "#523178"' in src
    assert '"CMR Falabella": "#3B9326"' in src
    assert 'Falabella: "#3B9326"' in src
    assert 'CMF_FALABELLA: "#3B9326"' in src
    assert 'BCI: "#FFD200"' in src
    assert 'Itaú: "#EC7000"' in src
    assert '"Banco Itaú": "#EC7000"' in src
    assert 'Internacional: "#D90D39"' in src
    assert 'Santander: "#EC0000"' in src
    assert 'Security: "#6A2E92"' in src
    assert 'BICE: "#1976D2"' in src
    assert "getHashedBankColor(institutionCode)" in src


def test_chart_and_selector_use_name_aware_bank_color_lookup():
    chart_src = Path("front/components/metric-line-chart.tsx").read_text()
    selector_src = Path("front/components/bank-selector.tsx").read_text()

    assert (
        "getBankColor(\n                  bank.institutionCode,\n                  bank.institutionName\n                )"
        in chart_src
    )
    assert "getBankColor(bank.institutionCode, bank.institutionName)" in selector_src
