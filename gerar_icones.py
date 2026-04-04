"""
Gera os ícones PNG necessários para o PWA.
Execute: pip install pillow && python gerar_icones.py
"""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

SIZES = [144, 180, 192, 512]
OUT_DIR = Path("static/icons")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Paleta do app
BG_COLOR    = (26, 29, 39)    # #1a1d27
RING_COLOR  = (44, 48, 72)    # anel externo
FACE_COLOR  = (124, 134, 255) # #7c86ff (azul do tema)
SKIN_COLOR  = (220, 190, 160) # tom de pele neutro


def draw_icon(size: int) -> Image.Image:
    img = Image.new("RGBA", (size, size), BG_COLOR + (255,))
    d   = ImageDraw.Draw(img)
    s   = size

    # Fundo circular com anel
    margin = int(s * 0.04)
    d.ellipse([margin, margin, s - margin, s - margin], fill=RING_COLOR)

    # Silhueta da cabeça (círculo central)
    head_r = int(s * 0.22)
    cx, cy_head = s // 2, int(s * 0.38)
    d.ellipse(
        [cx - head_r, cy_head - head_r, cx + head_r, cy_head + head_r],
        fill=SKIN_COLOR,
    )

    # Silhueta do corpo (elipse inferior)
    bw, bh = int(s * 0.38), int(s * 0.22)
    cy_body = int(s * 0.72)
    d.ellipse(
        [cx - bw, cy_body - bh, cx + bw, cy_body + bh],
        fill=SKIN_COLOR,
    )

    # Corta o excesso fora do círculo principal
    mask = Image.new("L", (s, s), 0)
    md   = ImageDraw.Draw(mask)
    md.ellipse([margin, margin, s - margin, s - margin], fill=255)
    img.putalpha(mask)

    # Anel de destaque (borda colorida)
    ring_w = max(2, int(s * 0.035))
    for i in range(ring_w):
        d.ellipse(
            [margin + i, margin + i, s - margin - i, s - margin - i],
            outline=FACE_COLOR + (220,),
        )

    # Achata RGBA → RGB com fundo sólido.
    # iOS não suporta transparência em apple-touch-icon: fundo transparente
    # resulta em ícone preto ou ausente na tela inicial.
    bg = Image.new("RGB", (s, s), BG_COLOR)
    bg.paste(img, mask=img.split()[3])
    return bg


for sz in SIZES:
    path = OUT_DIR / f"icon-{sz}.png"
    icon = draw_icon(sz)
    icon.save(path, "PNG")
    print(f"  Gerado: {path}  ({sz}×{sz})")

print("\nIcones gerados com sucesso em static/icons/")
print("Dica: substitua pelos seus ícones reais quando tiver o design final.")
