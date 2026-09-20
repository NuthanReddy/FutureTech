from pathlib import Path

import qrcode
from PIL import Image
from qrcode.util import pattern_position

from Utils.QrGenerator import QrGenerator


LONG_UPI_PAYLOAD = (
    "upi://pay?pa=lenskartsolutionspvtdd1.76028835@hdfcban"
    "&pn=Lenskart Solutions Limited (formerly known as Lenskart Solutions Private Limited)"
    "&am=7799.0&tn=1350130623"
    "&invoiceDetail={gstNumber:08AACCV7324B1ZK,bankName:HDFC BANK LTD,"
    "accountNumber:50200066281830,ifscCode:HDFC0000396,invoiceNumber:IIN10826B1863301,"
    "invoiceDate:02/09/2026,igst:392.26,cgst:0.0,sgst:0.0,utgst:0.0,cess:0.0}"
)


def _open_image(path: Path) -> Image.Image:
    image = Image.open(path)
    image.load()
    return image


def _has_black_pixel(image: Image.Image, box: tuple[int, int, int, int]) -> bool:
    """Return whether the given image box contains any black QR module pixels."""
    left, top, right, bottom = box
    for x_position in range(left, right):
        for y_position in range(top, bottom):
            if image.getpixel((x_position, y_position)) == 0:
                return True
    return False


def _quadrant_boxes(image: Image.Image) -> list[tuple[int, int, int, int]]:
    """Return Q1, Q2, Q3, and Q4 boxes for one complete QR image."""
    mid_x = image.width // 2
    mid_y = image.height // 2
    return [
        (0, 0, mid_x, mid_y),
        (mid_x, 0, image.width, mid_y),
        (0, mid_y, mid_x, image.height),
        (mid_x, mid_y, image.width, image.height),
    ]


def test_generate_creates_scaled_png(tmp_path: Path) -> None:
    """Generated QR images should use module_size pixels per QR module."""
    output = tmp_path / "qr.png"

    path = QrGenerator(module_size=4, border=4).generate("https://example.com", output)

    assert path == output
    assert output.exists()

    image = _open_image(output)
    assert image.format == "PNG"
    assert image.mode == "L"
    assert image.width == image.height
    assert image.width % 4 == 0
    assert image.width > 50


def test_module_size_scales_output_dimensions(tmp_path: Path) -> None:
    """The same payload should scale linearly with module_size."""
    small_path = tmp_path / "small.png"
    large_path = tmp_path / "large.png"
    payload = "scale-me"

    QrGenerator(module_size=2, border=4).generate(payload, small_path)
    QrGenerator(module_size=6, border=4).generate(payload, large_path)

    small_image = _open_image(small_path)
    large_image = _open_image(large_path)
    assert large_image.size == (small_image.width * 3, small_image.height * 3)


def test_generate_preserves_quiet_zone(tmp_path: Path) -> None:
    """The quiet zone around the QR code must remain white for reliable scanning."""
    module_size = 5
    border = 4
    quiet_zone_pixels = module_size * border
    output = tmp_path / "quiet-zone.png"

    QrGenerator(module_size=module_size, border=border).generate("quiet zone", output)

    image = _open_image(output)

    for x_position in range(image.width):
        for y_position in range(quiet_zone_pixels):
            assert image.getpixel((x_position, y_position)) == 255

    for x_position in range(quiet_zone_pixels):
        for y_position in range(image.height):
            assert image.getpixel((x_position, y_position)) == 255


def test_generate_creates_nested_output_for_unicode_payload(tmp_path: Path) -> None:
    """Unicode payloads and nested output directories should work."""
    output = tmp_path / "nested" / "unicode.png"

    path = QrGenerator().generate("Hello, नमस्ते, こんにちは", output)

    assert path == output
    assert output.exists()
    assert _open_image(output).width > 0


def test_constructor_validates_dimensions() -> None:
    """Invalid module and border sizes should fail fast."""
    invalid_cases = [
        (0, 4, "module_size must be at least 1"),
        (-1, 4, "module_size must be at least 1"),
        (1, -1, "border cannot be negative"),
    ]

    for module_size, border, message in invalid_cases:
        try:
            QrGenerator(module_size=module_size, border=border)
        except ValueError as exc:
            assert message in str(exc)
        else:
            raise AssertionError("QrGenerator accepted invalid dimensions")


def test_min_version_can_use_smallest_possible_qr(tmp_path: Path) -> None:
    """Passing min_version=None keeps compact QR generation available."""
    output = tmp_path / "compact.png"

    generator = QrGenerator(module_size=3, border=4, min_version=None)
    path = generator.generate("short", output)

    image = _open_image(path)
    expected_version = qrcode.QRCode(version=None, border=4)
    expected_version.add_data("short")
    expected_version.make(fit=True)
    expected_modules = 21 + 4 * (expected_version.version - 1) + 2 * generator.border
    assert image.size == (expected_modules * generator.module_size, expected_modules * generator.module_size)


def test_long_payload_generates_one_complete_qr_with_four_populated_quadrants(tmp_path: Path) -> None:
    """A full QR should contain data across Q1, Q2, Q3, and Q4 of one image."""
    output = tmp_path / "full-qr.png"
    generator = QrGenerator(module_size=2)

    path = generator.generate(LONG_UPI_PAYLOAD, output)

    image = _open_image(path)
    qr = generator._build_qr(LONG_UPI_PAYLOAD)
    assert image.width == image.height
    assert qr.version == 13
    assert len(pattern_position(qr.version)) == 3
    assert all(_has_black_pixel(image, box) for box in _quadrant_boxes(image))


def test_error_correction_can_force_larger_marker_grid() -> None:
    """Higher error correction remains available, but may need four markers per row."""
    qr = QrGenerator(error_correction=qrcode.constants.ERROR_CORRECT_M)._build_qr(LONG_UPI_PAYLOAD)

    assert qr.version == 15
    assert len(pattern_position(qr.version)) == 4


def test_min_version_can_force_larger_qr_when_needed(tmp_path: Path) -> None:
    """Callers can still force a minimum QR version explicitly."""
    output = tmp_path / "forced-version.png"
    min_version = 6

    generator = QrGenerator(module_size=2, border=4, min_version=min_version)
    path = generator.generate("short", output)

    image = _open_image(path)
    expected_modules = 21 + 4 * (min_version - 1) + 2 * generator.border
    assert image.size == (expected_modules * generator.module_size, expected_modules * generator.module_size)
    assert generator._build_qr("short").version == min_version


def test_constructor_validates_min_version() -> None:
    """Invalid minimum QR versions should fail fast."""
    invalid_cases = [0, -1, 41]

    for min_version in invalid_cases:
        try:
            QrGenerator(min_version=min_version)
        except ValueError as exc:
            assert "min_version must be between 1 and 40, or None" in str(exc)
        else:
            raise AssertionError("QrGenerator accepted an invalid min_version")


def test_constructor_validates_error_correction() -> None:
    """Unsupported QR error-correction levels should fail fast."""
    try:
        QrGenerator(error_correction=999)
    except ValueError as exc:
        assert "error_correction must be one of L, M, Q, or H" in str(exc)
    else:
        raise AssertionError("QrGenerator accepted an invalid error correction level")
