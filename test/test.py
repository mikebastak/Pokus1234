import ftplib
import io
import logging
from lxml import etree
import requests

# Configure logging
type = logging.INFO
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

# --- FTP CONFIGURATION ---
FTP_HOST = 'ftp.dealer.2service.nl'
FTP_USER = '641790'
FTP_PASS = 'rG$rG?1v'
FTP_FILE = 'parts.xml'

# --- Base.com API CONFIGURATION ---
API_TOKEN = '5011951-5044692-5K0UFLZ1VVUMKFMJQH7RCJMDJUS8587IWM2Y5XL3DJHUYU4OJXBX0JKAIRK6NPIV'
# Replace with your actual Base.com endpoint
BASE_API_URL = 'https://api.base.com/parts'
HEADERS = {
    'Authorization': f'Bearer {API_TOKEN}',
    'Content-Type': 'application/json'
}


def download_parts_xml() -> io.BytesIO:
    """
    Connects to the FTP server and retrieves the parts.xml file into a BytesIO buffer.
    """
    logger.info('Connecting to FTP %s', FTP_HOST)
    ftp = ftplib.FTP(FTP_HOST)
    ftp.login(FTP_USER, FTP_PASS)

    buffer = io.BytesIO()
    logger.info('Starting download of %s', FTP_FILE)
    ftp.retrbinary(f'RETR {FTP_FILE}', buffer.write)
    ftp.quit()
    buffer.seek(0)
    logger.info('Download complete, size=%s bytes', len(buffer.getvalue()))
    return buffer


def stream_and_post(buffer: io.BytesIO):
    """
    Parses the XML in a streaming fashion and posts each <part> record to Base.com API.
    Clears elements from memory to keep footprint low.
    """
    logger.info('Beginning XML parse and upload')
    context = etree.iterparse(buffer, events=('end',), tag='part')
    count = 0

    for event, elem in context:
        # Build payload mapping fields from XML to API JSON
        payload = {
            'partId':           elem.findtext('part_id'),
            'manufacturerCode': elem.findtext('manufacturer_article_code'),
            'partNumber':       elem.findtext('part_number'),
            'description':      elem.findtext('description'),
            'unitPrice':        float(elem.findtext('unit_price') or 0),
            'stockQuantity':    int(elem.findtext('stock_quantity') or 0),
            'canBeOrdered':     elem.findtext('can_be_ordered') == 'true',
            'qualityId':        elem.findtext('quality_id'),
            # Add or remove fields as required by Base.com API
        }

        response = requests.post(BASE_API_URL, json=payload, headers=HEADERS)
        if not response.ok:
            logger.error('Failed to POST part %s: %s %s', payload['partId'], response.status_code, response.text)
        else:
            logger.debug('Posted part %s successfully', payload['partId'])

        count += 1
        # Clear processed element to free memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

        if count % 1000 == 0:
            logger.info('Processed %d parts', count)

    logger.info('Finished processing XML. Total parts uploaded: %d', count)
    del context


def main():
    try:
        xml_buffer = download_parts_xml()
        stream_and_post(xml_buffer)
    except Exception as e:
        logger.exception('Error during processing: %s', e)


if __name__ == '__main__':
    main()
