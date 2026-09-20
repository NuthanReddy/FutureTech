# python
from __future__ import annotations

from pathlib import Path

import qrcode
from PIL import Image, ImageDraw


class QrGenerator:
    """Generate standards-compliant QR code PNG images for text payloads."""

    VALID_ERROR_CORRECTION_LEVELS = {
        qrcode.constants.ERROR_CORRECT_L,
        qrcode.constants.ERROR_CORRECT_M,
        qrcode.constants.ERROR_CORRECT_Q,
        qrcode.constants.ERROR_CORRECT_H,
    }

    def __init__(
        self,
        module_size: int = 10,
        border: int = 4,
        min_version: int | None = None,
        error_correction: int = qrcode.constants.ERROR_CORRECT_L,
    ):
        """Create a QR generator.

        Args:
            module_size: Pixel width/height of one QR module.
            border: Quiet-zone width in QR modules. A value of 4 is recommended.
            min_version: Optional minimum QR version from 1 to 40. Use ``None``
                to pick the smallest QR version that fits the complete payload.
            error_correction: QR error-correction constant. The default ``L``
                keeps dense payloads smaller, which gives the sample QR three
                alignment markers per row instead of four.

        Raises:
            ValueError: If dimensions are invalid, ``min_version`` is out of
                range, or ``error_correction`` is unsupported.
        """
        if module_size < 1:
            raise ValueError("module_size must be at least 1")
        if border < 0:
            raise ValueError("border cannot be negative")
        if min_version is not None and not 1 <= min_version <= 40:
            raise ValueError("min_version must be between 1 and 40, or None")
        if error_correction not in self.VALID_ERROR_CORRECTION_LEVELS:
            raise ValueError("error_correction must be one of L, M, Q, or H")

        self.module_size = module_size
        self.border = border
        self.min_version = min_version
        self.error_correction = error_correction

    def _build_qr(self, text: str) -> qrcode.QRCode:
        """Build a QRCode object sized for the payload."""
        qr = qrcode.QRCode(
            version=self.min_version,
            error_correction=self.error_correction,
            box_size=self.module_size,
            border=self.border,
        )
        qr.add_data(text)
        qr.make(fit=True)
        return qr

    def _render_matrix(self, matrix: list[list[bool]]) -> Image.Image:
        """Render a QR module matrix to a grayscale PIL image."""
        module_count = len(matrix)
        image_size = module_count * self.module_size
        image = Image.new("L", (image_size, image_size), 255)
        draw = ImageDraw.Draw(image)

        for row_index, row in enumerate(matrix):
            top = row_index * self.module_size
            for col_index, bit in enumerate(row):
                if not bit:
                    continue

                left = col_index * self.module_size
                draw.rectangle(
                    (
                        left,
                        top,
                        left + self.module_size - 1,
                        top + self.module_size - 1,
                    ),
                    fill=0,
                )

        return image

    def _save_image(self, image: Image.Image, output_path: str | Path) -> Path:
        """Save a rendered image as PNG, creating parent directories."""
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        image.save(output, format="PNG")
        return output

    def generate(self, text: str, output_path: str | Path = "qr.png") -> Path:
        """Generate a scannable QR code PNG image for the provided text.

        Args:
            text: Text payload to encode.
            output_path: Destination PNG path. Parent directories are created.

        Returns:
            The path where the PNG was written.
        """
        qr = self._build_qr(text)
        matrix = qr.get_matrix()
        image = self._render_matrix(matrix)
        return self._save_image(image, output_path)


if __name__ == "__main__":
    # Generate one complete QR code. If you mentally split this final PNG into
    # four equal image quadrants, Q1 is the top-left quarter of this same QR.
    upi_payload = (
        "https://nuthanreddy.github.io/HomePlanner/"
    )
    generator = QrGenerator()
    output_path = Path(__file__).with_name("output") / "qr.png"
    generator.generate(upi_payload, output_path)
