# encoding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os.path
import sys
from optparse import make_option
from socket import getfqdn

from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand, CommandError
from django.template import Context, loader
from django.utils.six.moves.urllib.parse import SplitResult, urlsplit, urlunsplit

from haystack import constants
from haystack.backends.solr_backend import SolrSearchBackend


class Command(BaseCommand):
    help = "Generates a Solr schema that reflects the indexes."
    base_options = (
        make_option("-f", "--filename", action="store", type="string", dest="filename",
                    help='If provided, directs output to a file instead of stdout.'),
        make_option("-u", "--using", action="store", type="string", dest="using",
                    default=constants.DEFAULT_ALIAS,
                    help='If provided, chooses a connection to work with.'),
        make_option("-s", "--solr-version", action="store", type="string",
                    dest="solr_version", metavar='0[.0][.0]',
                    help='Specify targeted Solr version for schema to use. '
                         'Usually this is not needed as version is checked through '
                         'Solr API, however if core is not running for whatever '
                         'reason this must be used instead. Format of value can be '
                         "any one of these: '5', '5.3', '5.3.0'."),
        make_option("-c", "--commit-local", action="store_true", dest="commit",
                    help="If used, schema is written straight to target core's"
                         " 'conf' directory (only use with local Solr instance"
                         " and make sure that file permissions allow that)."
                         " Afterwards core is reloaded to make schema changes"
                         " effective.")
    )
    option_list = BaseCommand.option_list + base_options

    def handle(self, **options):
        """Generates a Solr schema that reflects the indexes."""
        from haystack import connections
        from pysolr import SolrCoreAdmin, SolrError

        using = options.get('using')
        self.connection = connections[using]
        self.backend = self.connection.get_backend()
        if not isinstance(self.backend, SolrSearchBackend):
            raise ImproperlyConfigured("'%s' isn't configured as a SolrEngine)." % self.backend.connection_alias)
        try:
            self.solr_info = self.backend.conn.system_info()
        except SolrError:
            self.solr_info = {}
        solr_version = options.get('solr_version')
        if solr_version:
            self.solr_info['lucene'] = self.solr_info.get('lucene', {})
            self.solr_info['lucene']['solr-spec-version'] = solr_version

        schema_xml = self.build_template(using=using)

        if options.get('commit'):
            try:
                filename = self.solr_info['core']['directory']['instance']
                solr_fqdn = self.solr_info['core']['host']
            except KeyError:
                raise CommandError('Could not determine core\'s directory')
            if solr_fqdn != getfqdn():
                raise CommandError("Solr host is '%s' while local hostname is '%s'."
                                   " Cannot use --commit-local!" % (solr_fqdn, getfqdn()))
            filename = os.path.join(filename, 'conf', 'schema.xml')
            self.write_file(filename, schema_xml)

            u = urlsplit(self.connection.options['URL'])
            url_path = u.path.rstrip('/')
            url_i = url_path.rfind('/')
            core_name = url_path[url_i+1:]
            base_path = u.path[:url_i]
            base_url = SplitResult(u.scheme, u.netloc, base_path, '', '')
            core_admin = SolrCoreAdmin(urlunsplit(base_url))
            core_admin.reload(core_name)
        elif options.get('filename'):
            self.write_file(options.get('filename'), schema_xml)
        else:
            self.print_stdout(schema_xml)

    def build_context(self, using):
        # get running Solr version as a tuple
        solr_version = (1, 0, 0)  # silly default
        try:
            version_str = self.solr_info['lucene']['solr-spec-version']
            solr_version = [int(i) for i in version_str.split('.')]
            # do some additional checks/cleanups for version string provided as
            # command line argument
            if not len(solr_version):
                raise KeyError()
            while len(solr_version) < 3:
                solr_version.append(0)
        except KeyError:
            raise CommandError('Could not determine Solr version. Use --solr-version.')
        version_shortcut = 'solr_%s' % (solr_version[0])

        content_field_name, fields = self.backend.build_schema(
            self.connection.get_unified_index().all_searchfields())
        return Context({
            'solr_info': self.solr_info,
            'solr_version': solr_version,
            version_shortcut: True,
            'connection_alias': using,
            'content_field_name': content_field_name,
            'fields': fields,
            'default_operator': constants.DEFAULT_OPERATOR,
            'ID': constants.ID,
            'DJANGO_CT': constants.DJANGO_CT,
            'DJANGO_ID': constants.DJANGO_ID,
        })

    def build_template(self, using):
        t = loader.get_template('search_configuration/solr.xml')
        c = self.build_context(using=using)
        return t.render(c)

    def print_stdout(self, schema_xml):
        sys.stderr.write("\n")
        sys.stderr.write("\n")
        sys.stderr.write("\n")
        sys.stderr.write("Save the following output to 'schema.xml' and place it in your Solr configuration directory.\n")
        sys.stderr.write("--------------------------------------------------------------------------------------------\n")
        sys.stderr.write("\n")
        print(schema_xml)

    def write_file(self, filename, schema_xml):
        schema_file = open(filename, 'w')
        schema_file.write(schema_xml)
        schema_file.close()
