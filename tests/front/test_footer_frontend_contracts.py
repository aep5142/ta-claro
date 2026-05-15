from pathlib import Path


def test_homepage_and_app_shell_use_shared_site_footer():
    homepage_src = Path("front/components/homepage.tsx").read_text()
    shell_src = Path("front/components/app-shell.tsx").read_text()
    footer_src = Path("front/components/site-footer.tsx").read_text()

    assert "export function SiteFooter" in footer_src
    assert "Instant benchmark for Chilean banking." in footer_src
    assert "<SiteFooter />" in homepage_src
    assert "<SiteFooter />" in shell_src
