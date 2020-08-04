import unittest
import os
import logging

from kafka_client import KafkaImageCli
import video_streamer
from video_consumer import consume_images_from_kafka

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class TestVideoConsumer(unittest.TestCase):
    def test_detect_known_faces(self):
        inside_container= os.environ.get("RUN_TESTS_INSIDE_CONTAINER", False)
        logger.debug("Environment variable RUN_TESTS_INSIDE_CONTAINER: {v}".format(v=inside_container))
        if inside_container:
            bootstrap_servers= ['kafka:29092']
        else:
            bootstrap_servers= ['localhost:9092']
        
        videofile= "./testvideo.webm"
        kafkatopic= "tests"
        outdir= "/tmp/VideoAnalytics/out"
        facesDir= "/tmp/VideoAnalytics/faces"

        #todo: create temp diretories
        assert os.path.exists(outdir) and os.path.exists(facesDir)

        video_streamer.stream_video_from_file(videofile, kafkatopic,bootstrap_servers)

        kafkaCli = KafkaImageCli(
            bootstrap_servers=bootstrap_servers,
            topic=kafkatopic,
            stop_iteration_timeout=3000)


        kafkaCli.register_consumer()
        matched_titles = consume_images_from_kafka(kafkaCli, 
                                                   known_faces_path= facesDir,
                                                   outpath= outdir)
        print(matched_titles)
        #self.assertEqual(
        #    set(['Manoj_direct', 'Manoj_with_beard', 'Manoj_US_Visa']), matched_titles)


if __name__ == "__main__":
    unittest.main()
