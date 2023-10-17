import json
import re
import subprocess
from logging import Logger
from pathlib import Path
from typing import Any, Optional, Tuple

from acacore.models.identification import Identification
from acacore.reference_files.ref_files import costum_sigs, to_re_identify


def aca_id_for_file_id(path: Path, file_id: Identification) -> Identification:
    """Uses the BOF and EOF to try to determine a ACAUID for a file and update its Identification datastructure.

    If none can be found, simply return the same Identification data structure as it got in the beginning.

    Args:
        path (Path): PAth to the file
        file_id (Identification): The file identification data structure that should be updated with the new values

    Returns:
        Identification: The updated file data structure.
    """
    bof, eof = get_bof_and_eof(path)

    sig_for_file = get_aca_signature(bof, eof)
    if sig_for_file:
        update_file_id(path, file_id, sig_for_file)
    return file_id


def aca_id(path: Path) -> Optional[str]:
    """Tries to find one of our costum ACAUID's for a file, based on its BOF and EOF bytes. Returns `None` if none is found.

    Args:
        path (Path): Path to the file to be examined

    Returns:
        Optional[str]: Possible ACAUID
    """
    bof, eof = get_bof_and_eof(path)

    sig_for_file = get_aca_signature(bof, eof)
    if not sig_for_file:
        return None

    return sig_for_file.get("puid", None)


def get_bof_and_eof(file: Path) -> Tuple[str, str]:
    """Get the first and last kilobyte of a file.

    Args:
        file (Path): Path to file

    Returns:
        Tuple[str,str]: BOF and then EOF as `str`.
    """
    with file.open("rb") as file_bytes:
        # BOF
        bof = file_bytes.read(1024).hex()
        # Navigate to EOF
        try:
            file_bytes.seek(-1024, 2)
        except OSError:
            # File too small :)
            file_bytes.seek(-file_bytes.tell(), 2)
        eof = file_bytes.read(1024).hex()
    return (bof, eof)


def get_aca_signature(bof: str, eof: str) -> Optional[dict]:
    """Get the ACA signature of a file type, if one exists. Else return `None`.

    Args:
        bof (str): The first kilobyte of a file
        eof (str): The last kilobyte of a file

    Returns:
        Optional(str): The signature, if one was found.
    """
    aca_signatures: list[dict] = costum_sigs()
    for sig in aca_signatures:
        if "bof" in sig and "eof" in sig:
            bof_pattern = re.compile(sig["bof"])
            eof_pattern = re.compile(sig["eof"])
            if sig["operator"] == "OR":
                if bof_pattern.search(bof) or eof_pattern.search(eof):
                    return sig
            elif sig["operator"] == "AND" and bof_pattern.search(bof) and eof_pattern.search(eof):
                return sig
        elif "bof" in sig:
            bof_pattern = re.compile(sig["bof"])
            if bof_pattern.search(bof):
                return sig
        elif "eof" in sig:
            eof_pattern = re.compile(sig["eof"])
            if eof_pattern.search(eof):
                return sig
    return None


def sf_id_full(path: Path, log: Optional[Logger] = None) -> dict[Path, Identification]:
    """Identify multiple files using `siegfried`, and return a dictionary mapping the files path to a Identification datastructure containing the info obtained.

    Also updates FileInfo with obtained PUID, signature name, and warning if applicable.

    Parameters
    ----------
    path : pathlib.Path
        Path in which to identify files.

    Returns:
    -------
    Dict[Path, Identification]
        Dictionary containing file path and associated identification
        information obtained from siegfried's stdout.

    """
    id_dict: dict[Path, Identification] = {}

    id_result = run_sf_and_get_results_json(path)

    # We get identifiers as a list containing the ditionary,
    # soo we have to get the one element our of it
    results_dict: Optional[dict] = id_result.get("identifiers", None)[0]
    if results_dict and log:
        DROID_file_version: Optional[str] = results_dict.get("details")
        log.info(
            "Running sf with the following version of DROID: " + DROID_file_version if DROID_file_version else "",
        )
    for file_result in id_result.get("files", []):
        match: dict[str, Any] = {}
        for id_match in file_result.get("matches"):
            if id_match.get("ns") == "pronom":
                match = id_match
        if match:
            file_identification: Identification
            file_path: Path = Path(file_result["filename"])

            puid = None if match.get("id", "").lower() == "unknown" else match.get("id")

            signature_and_version = None
            signature = match.get("format")
            version = match.get("version")
            if signature:
                signature_and_version = f"{signature} ({version})"
            warning: str = match.get("warning", "").capitalize()
            file_size: int = file_result.get("filesize")
            file_errors: Optional[str] = file_result.get("errors", None)
            if file_errors:
                warning = warning + " ; Errors: " + file_errors
            file_identification = Identification(
                puid=puid,
                signature=signature_and_version or None,
                warning=warning or None,
                size=file_size,
            )

            # unindentified files
            if puid is None:
                file_identification = aca_id_for_file_id(file_path, file_identification)

            # re-identify files, warnings or not!
            if puid in to_re_identify():
                file_identification = aca_id_for_file_id(file_path, file_identification)

            # Possible MS Office files identified as markup (XML, HTML etc.)
            if puid in ["fmt/96", "fmt/101", "fmt/583", "x-fmt/263"] and "Extension mismatch" in warning:
                file_identification = aca_id_for_file_id(file_path, file_identification)

            id_dict.update({file_path: file_identification})

    return id_dict


# ---
# Aux. methods, used as helper methods for the rest of the methods.
# ---


def run_sf_and_get_results_json(path: Path) -> dict:
    """Run `siegfried` on `path`, and return the result as a dictionary build from the .json output of `sf`.

    Args:
        path (Path): A path to a folder containg files or subfolder with files (or more subfolders! )

    Raises:
        OSError: If there is an error with running siegfried or loading the results as a .json file, raises OSError

    Returns:
        dict: dictionary created from .json output of siegfried
    """
    try:
        sf_proc = subprocess.run(
            ["sf", "-json", "-multi", "1024", str(path)],
            check=True,
            capture_output=True,
        )
    except Exception as error:
        raise OSError(error)

    try:
        id_result: dict = json.loads(sf_proc.stdout)
    except Exception as error:
        raise OSError(error)

    return id_result


def update_file_id(path: Path, file_id: Identification, signature: dict[str, str]) -> None:
    """Update a file Identification data model with an PUID and signature given as a dictionary.

    It is primarily used by the `costum_id` method.
    Also adds a 'Extension Mismatch' warning if the extension of the file is not as we excpect from the given dict.

    Args:
        path (Path): Path to the file
        file_id (Identification): File identification data model
        signature (dict[str, str]): Dictionary with new values for PUID and signature.
    """
    file_id.puid = signature["puid"]
    file_id.signature = signature["signature"]
    if path.suffix.lower() != signature["extension"].lower():
        file_id.warning = "Extension mismatch"
    else:
        file_id.warning = None
