"""Test merkle path verification logic using real CAR test data"""

import pytest
from pathlib import Path
from multiformats import CID
from ipfsspec.async_ipfs import AsyncIPFSGateway
from ipfsspec.car import read_car


# Test data from test/testdata.car
# Root: QmW3CrGFuFyF3VH1wvrap4Jend5NRTgtESDjuQ7QhHD5dd (directory)
# Children:
#   - default: QmZsn2gmGC6yBs6TWPiRspXfTJ3K4DEtWUePVqBJ84YkU8
#   - multi: QmaSgZFgGWWuV27GG1QtZuqTXrdWM5yLLdtyr5SSutmJFr
#   - raw: bafkreibauudqsswbcktzrs5bwozj3cllhme56jlj23op4lwgmsucpv222q
#   - raw_multi: QmeMPrSpm7q5bjczEJLPRHiSDdwEPWt16phrBUx2YY4E8g
#   - write: QmUHyXsVBDM9qkj4aaBrqcm12eFYPWva2jmAMD5TJfp2Qh


@pytest.fixture
def test_car():
    """Load blocks and root CID from test/testdata.car"""
    root_cid = CID.decode("QmW3CrGFuFyF3VH1wvrap4Jend5NRTgtESDjuQ7QhHD5dd")
    car_path = Path(__file__).parent / "testdata.car"
    with open(car_path, "rb") as f:
        car_data = f.read()

    _, blocks_iter = read_car(car_data)
    blocks = {cid: data for cid, data, _ in blocks_iter}
    return root_cid, blocks


def test_verify_merkle_path_single_cid(test_car):
    """Test verification with just a single CID (no path)"""
    root_cid, blocks = test_car

    # Should return the same CID for a path with no segments
    result = AsyncIPFSGateway._verify_merkle_path(str(root_cid), blocks)
    assert result == root_cid


def test_verify_merkle_path_valid_child(test_car):
    """Test verification of a valid path to a child entry"""
    root_cid, blocks = test_car
    expected_child_cid = CID.decode("QmZsn2gmGC6yBs6TWPiRspXfTJ3K4DEtWUePVqBJ84YkU8")

    # Verify path to "default" entry
    result = AsyncIPFSGateway._verify_merkle_path(
        f"{root_cid}/default", blocks
    )
    assert result == expected_child_cid


def test_verify_merkle_path_valid_cidv1_child(test_car):
    """Test verification with CIDv1 child (raw block)"""
    root_cid, blocks = test_car
    expected_raw_cid = CID.decode(
        "bafkreibauudqsswbcktzrs5bwozj3cllhme56jlj23op4lwgmsucpv222q"
    )

    # Verify path to "raw" entry (CIDv1)
    result = AsyncIPFSGateway._verify_merkle_path(f"{root_cid}/raw", blocks)
    assert result == expected_raw_cid


def test_verify_merkle_path_all_children(test_car):
    """Test verification of all child entries in the directory"""
    root_cid, blocks = test_car

    expected_entries = {
        "default": "QmZsn2gmGC6yBs6TWPiRspXfTJ3K4DEtWUePVqBJ84YkU8",
        "multi": "QmaSgZFgGWWuV27GG1QtZuqTXrdWM5yLLdtyr5SSutmJFr",
        "raw": "bafkreibauudqsswbcktzrs5bwozj3cllhme56jlj23op4lwgmsucpv222q",
        "raw_multi": "QmeMPrSpm7q5bjczEJLPRHiSDdwEPWt16phrBUx2YY4E8g",
        "write": "QmUHyXsVBDM9qkj4aaBrqcm12eFYPWva2jmAMD5TJfp2Qh",
    }

    for name, expected_cid_str in expected_entries.items():
        expected_cid = CID.decode(expected_cid_str)
        result = AsyncIPFSGateway._verify_merkle_path(
            f"{root_cid}/{name}", blocks
        )
        assert result == expected_cid, f"Failed for entry '{name}'"


def test_verify_merkle_path_missing_root():
    """Test that missing root block raises FileNotFoundError"""
    cid = CID.decode("QmW3CrGFuFyF3VH1wvrap4Jend5NRTgtESDjuQ7QhHD5dd")
    blocks = {}  # Empty, root not present

    with pytest.raises(FileNotFoundError, match="Root block .* not found"):
        AsyncIPFSGateway._verify_merkle_path(str(cid), blocks)


def test_verify_merkle_path_invalid_root_cid():
    """Test that invalid CID in path raises FileNotFoundError"""
    blocks = {}

    with pytest.raises(FileNotFoundError, match="Invalid root CID"):
        AsyncIPFSGateway._verify_merkle_path("not-a-valid-cid/path", blocks)


def test_verify_merkle_path_nonexistent_path_segment(test_car):
    """Test that nonexistent path segment raises FileNotFoundError"""
    root_cid, blocks = test_car

    with pytest.raises(FileNotFoundError, match="Path segment 'nonexistent' not found"):
        AsyncIPFSGateway._verify_merkle_path(f"{root_cid}/nonexistent", blocks)


def test_verify_merkle_path_wrong_segment_name(test_car):
    """Test that wrong path segment name raises FileNotFoundError"""
    root_cid, blocks = test_car

    # "defaults" instead of "default"
    with pytest.raises(FileNotFoundError, match="Path segment 'defaults' not found"):
        AsyncIPFSGateway._verify_merkle_path(f"{root_cid}/defaults", blocks)


def test_verify_merkle_path_missing_intermediate_block(test_car):
    """Test that missing child block in chain raises FileNotFoundError"""
    root_cid, test_car = test_car

    # Create blocks dict with only root, missing child blocks
    blocks = {root_cid: test_car[root_cid]}

    with pytest.raises(FileNotFoundError, match="Child block .* not found"):
        AsyncIPFSGateway._verify_merkle_path(f"{root_cid}/default", blocks)
