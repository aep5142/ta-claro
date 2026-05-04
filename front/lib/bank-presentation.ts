export const bankDisplayNameMap: Record<string, string> = {
  "Servicios Financieros y Administración de Créditos Comerciales S.A. (SAG)": "Líder BCI",
  "SMU Corp S.A.": "Unicard",
  Scotiabank: "Scotiabank",
  "Banco Consorcio": "Consorcio",
  "Banco del Estado de Chile": "Banco Estado",
  "Banco de Chile": "Banco de Chile",
  "Banco París": "Banco París",
  "CAT Administradora de Tarjetas": "Cencosud Scotiabank",
  "CAR S.A.": "Tarjeta Ripley",
  BBVA: "BBVA",
  HSBC: "HSBC",
  "CMR Falabella S.A (SAG)": "CMR Falabella",
  BCI: "BCI",
  "Banco Itaú Chile": "Banco Itaú",
  "Banco Falabella": "Banco Falabella",
  "Banco Internacional": "Banco Internacional",
  "Banco Santander": "Banco Santander",
  "Banco Ripley": "Banco Ripley",
  "Consorcio Tarjetas de Crédito": "Tarjeta Consorcio",
  Coopeuch: "Coopeuch",
  "Banco Security": "Banco Security",
  Corpbanca: "Corpbanca",
  "Tenpo Payments S.A. - Tarjeta Mastercard": "Tenpo",
};

export function getBankDisplayName(name: string): string {
  return bankDisplayNameMap[name] ?? name;
}

const TENPO_RAW_NAME = "Tenpo Payments S.A. - Tarjeta Mastercard";

export function isTenpoInstitution(name: string): boolean {
  return name === TENPO_RAW_NAME;
}

export function isLikelyNonBankingInstitution(
  institutionName: string,
  institutionCode: string,
  sourceDatasetCode?: string
): boolean {
  if (sourceDatasetCode?.startsWith("bank_credit_card_ops_non_banking_")) {
    return true;
  }

  if (institutionCode === "MRC") {
    return true;
  }

  return institutionName.includes(" - Tarjeta ");
}

export function shouldIncludeInstitution(
  institutionName: string,
  institutionCode: string,
  sourceDatasetCode?: string
): boolean {
  const isNonBanking = isLikelyNonBankingInstitution(
    institutionName,
    institutionCode,
    sourceDatasetCode
  );
  return !isNonBanking || isTenpoInstitution(institutionName);
}

export function getBankColor(bankKey: string): string {
  let hash = 0;

  for (let index = 0; index < bankKey.length; index += 1) {
    hash = (hash * 31 + bankKey.charCodeAt(index)) >>> 0;
  }

  const hue = hash % 360;
  return `hsl(${hue} 72% 64%)`;
}
