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

const FALABELLA_RAW_NAMES = new Set([
  "CMR Falabella S.A (SAG)",
  "Promotora CMR Falabella S.A. - Tarjeta CMR Falabella",
  "Promotora CMR Falabella S.A. - Tarjeta Mastercard",
  "Promotora CMR Falabella S.A. - Tarjeta Visa",
]);

const FALABELLA_CANONICAL_CODE = "CMF_FALABELLA";
const FALABELLA_DISPLAY_NAME = "CMR Falabella";

export function getBankDisplayName(name: string): string {
  return bankDisplayNameMap[name] ?? name;
}

const TENPO_RAW_NAME = "Tenpo Payments S.A. - Tarjeta Mastercard";

export function isTenpoInstitution(name: string): boolean {
  return name === TENPO_RAW_NAME;
}

export function isFalabellaInstitution(name: string): boolean {
  return FALABELLA_RAW_NAMES.has(name);
}

export function getCanonicalInstitution(
  institutionName: string,
  institutionCode: string
): { institutionCode: string; institutionName: string } {
  if (isFalabellaInstitution(institutionName)) {
    return {
      institutionCode: FALABELLA_CANONICAL_CODE,
      institutionName: FALABELLA_DISPLAY_NAME,
    };
  }

  return {
    institutionCode,
    institutionName: getBankDisplayName(institutionName),
  };
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
  if (isFalabellaInstitution(institutionName)) {
    return true;
  }

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
