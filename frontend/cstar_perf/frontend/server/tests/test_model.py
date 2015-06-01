# -*- coding: utf-8 -*-
import unittest
import uuid
import string
import random
import time

from ..model import Model, Cluster, NoTestsScheduledError, APIKeyExistsError, UnknownAPIKeyError

## This isn't a true unit test, but an integration test against a real
## C* instance. It uses a separate keyspace so it shouldn't interfere
## with any existing data.

class TestModel(unittest.TestCase):
    def setUp(self):
        ks = "test_model_non_prod"
        cluster=Cluster(['127.0.0.1'])
        sess = cluster.connect()
        sess.execute("DROP KEYSPACE IF EXISTS {ks}".format(ks=ks))
        self.model = Model(cluster=cluster, keyspace=ks)

    def tearDown(self):
        sess = self.model.get_session()
        sess.execute("DROP KEYSPACE IF EXISTS {ks}".format(ks=self.model.keyspace))

    def test_instantiation(self):
        pass

    def test_status(self):
        m = self.model
        def mock_test_definition(title, cluster='bdplab', user='ryan'):
            return {'title': title,
                    'cluster': cluster,
                    'user': user}

        test1 = m.schedule_test(uuid.uuid1(), 'ryan', 'bdplab', mock_test_definition('1'))
        test2 = m.schedule_test(uuid.uuid1(), 'ryan', 'bdplab', mock_test_definition('2'))
        test3 = m.schedule_test(uuid.uuid1(), 'ryan', 'bdplab', mock_test_definition('3'))
        test4 = m.schedule_test(uuid.uuid1(), 'ryan', 'austin', mock_test_definition('1b'))
        test5 = m.schedule_test(uuid.uuid1(), 'ryan', 'bdplab', mock_test_definition('4'))
        test6 = m.schedule_test(uuid.uuid1(), 'ryan', 'bdplab', mock_test_definition('5'))
        test7 = m.schedule_test(uuid.uuid1(), 'ryan', 'bdplab', mock_test_definition('6'))
        test8 = m.schedule_test(uuid.uuid1(), 'ryan', 'bdplab', mock_test_definition('7'))
        test9 = m.schedule_test(uuid.uuid1(), 'ryan', 'austin', mock_test_definition('2b'))
        test10 = m.schedule_test(uuid.uuid1(), 'ryan', 'austin', mock_test_definition('3b'))
        test11 = m.schedule_test(uuid.uuid1(), 'ryan', 'austin', mock_test_definition('4b'))
        test12 = m.schedule_test(uuid.uuid1(), 'ryan', 'bdplab', mock_test_definition('8'))

        m.update_test_status(test1, 'in_progress')
        m.update_test_status(test2, 'in_progress')
        m.update_test_status(test4, 'in_progress')

        self.assertEquals([r['title'] for r in m.get_scheduled_tests('bdplab')], ['3','4','5','6','7','8'])
        self.assertEquals([r['title'] for r in m.get_scheduled_tests('bdplab',limit=2)], ['3','4'])
        self.assertEquals([r['title'] for r in m.get_scheduled_tests('austin')], ['2b','3b','4b'])
        self.assertEquals([r['title'] for r in m.get_in_progress_tests('bdplab')], ['1','2'])
        self.assertEquals([r['title'] for r in m.get_in_progress_tests('austin')], ['1b'])

        self.assertEquals(m.get_next_scheduled_test('bdplab')['title'], '3')
        self.assertEquals(m.get_next_scheduled_test('austin')['title'], '2b')
        self.assertRaises(NoTestsScheduledError, m.get_next_scheduled_test, 'no_such_cluster')

        m.update_test_status(test1, 'completed')
        m.update_test_status(test4, 'completed')
        self.assertEquals([r['title'] for r in m.get_completed_tests()], ['1b','1'])
        self.assertIsNotNone(m.get_completed_tests()[0]['completed_date'])


    def test_user_tests(self):
        m = self.model
        def mock_test_definition(title, cluster='bdplab', user='ryan'):
            return {'title': title,
                    'cluster': cluster,
                    'user': user}

        test1 = m.schedule_test(uuid.uuid1(), 'ryan', 'bdplab', mock_test_definition('1'))
        test2 = m.schedule_test(uuid.uuid1(), 'ryan', 'bdplab', mock_test_definition('2'))
        test3 = m.schedule_test(uuid.uuid1(), 'bob', 'bdplab', mock_test_definition('3'))
        test4 = m.schedule_test(uuid.uuid1(), 'mary', 'bdplab', mock_test_definition('4'))
        test5 = m.schedule_test(uuid.uuid1(), 'mary', 'bdplab', mock_test_definition('5'))

        self.assertEquals([r['title'] for r in m.get_user_scheduled_tests('ryan')], ['2','1'])
        self.assertEquals([r['title'] for r in m.get_user_scheduled_tests('bob')], ['3'])
        self.assertEquals([r['title'] for r in m.get_user_scheduled_tests('mary')], ['5','4'])
        
        m.update_test_status(test1, 'in_progress')
        m.update_test_status(test4, 'in_progress')

        self.assertEquals([r['title'] for r in m.get_user_scheduled_tests('ryan')], ['2'])
        self.assertEquals([r['title'] for r in m.get_user_scheduled_tests('bob')], ['3'])
        self.assertEquals([r['title'] for r in m.get_user_scheduled_tests('mary')], ['5'])

        self.assertEquals([r['title'] for r in m.get_user_in_progress_tests('ryan')], ['1'])
        self.assertEquals([r['title'] for r in m.get_user_in_progress_tests('mary')], ['4'])

        m.update_test_status(test2, 'completed')
        m.update_test_status(test5, 'completed')

        self.assertEquals([r['title'] for r in m.get_user_completed_tests('ryan')], ['2'])
        self.assertEquals([r['title'] for r in m.get_user_completed_tests('mary')], ['5'])
        

    def test_users(self):
        m = self.model
        m.create_user('jack@repairman.org', 'Repairman Jack', ['user','admin','repairman'])
        jack = m.get_user('jack@repairman.org')
        self.assertEquals(jack.user_id, 'jack@repairman.org')
        self.assertEquals(jack.full_name, 'Repairman Jack')
        self.assertEquals(jack.roles, set(['user','admin','repairman']))

        jack_pw = u"Jack's nifty passphrase with unicode - Ѧʋcн Ɯσω - Śő Múćĥ Ŵőŵ"
        m.set_user_passphrase(jack.user_id, jack_pw)
        self.assertTrue(m.validate_user_passphrase(jack.user_id, jack_pw))
        self.assertFalse(m.validate_user_passphrase(jack.user_id, "Not jack's passphrase"))
        
        jack_roles = m.get_user_roles('jack@repairman.org')
        self.assertEquals(jack_roles, set(['user','admin','repairman']))
    
    def test_artifacts(self):
        test_id = uuid.uuid1()
        m = self.model
        a1 = m.update_test_artifact(test_id, 'logs', 'LOG data 1', 'logs.tar.gz')
        a2 = m.update_test_artifact(test_id, 'logs-2', 'LOG data 2', 'more logs')
        a3 = m.update_test_artifact(test_id, 'graph', 'GRAPH data 1', 'stats.json')

        artifact_meta = m.get_test_artifact(test_id, 'logs')
        self.assertEqual(artifact_meta['artifact_type'], 'logs')
        artifact_meta = m.get_test_artifact(test_id, 'graph')
        self.assertEqual(artifact_meta['artifact_type'], 'graph')

        artifact_meta = m.get_test_artifacts(test_id)
        # artifacts are in alpha order by type:
        self.assertEqual(artifact_meta[0]['artifact_type'], 'graph')
        self.assertEqual(artifact_meta[1]['artifact_type'], 'logs')
        self.assertEqual(artifact_meta[2]['artifact_type'], 'logs-2')

        artifact = m.get_test_artifact_data(test_id, 'logs')
        self.assertEqual(artifact.artifact, 'LOG data 1')
        self.assertEqual(artifact.description, 'logs.tar.gz')
        artifact = m.get_test_artifact_data(test_id, 'logs-2')
        self.assertEqual(artifact.artifact, 'LOG data 2')
        self.assertEqual(artifact.description, 'more logs')
        artifact = m.get_test_artifact_data(test_id, 'graph')
        self.assertEqual(artifact.artifact, 'GRAPH data 1')
        self.assertEqual(artifact.description, 'stats.json')

    def test_clusters(self):
        m = self.model

        m.add_pub_key('new_cluster1', 'cluster', 'base64 encoded pubkey 1')
        m.add_pub_key('new_cluster2', 'cluster', 'base64 encoded pubkey 2')
        self.assertRaises(APIKeyExistsError, m.add_pub_key, 'new_cluster1', 'cluster', 'base64 encoded pubkey 1')
        
        self.assertEqual(m.get_pub_key('new_cluster1')['pubkey'], 'base64 encoded pubkey 1')
        self.assertRaises(UnknownAPIKeyError, m.get_pub_key, 'new_cluster_never_seen')

        m.add_cluster_jvm('new_cluster1', '1.7_65', '~/fab/java/1.7_65')
        self.assertEqual(m.get_clusters()['new_cluster1']['jvms'], {'1.7_65':'~/fab/java/1.7_65'})
