"""
    secedgartext: extract text from SEC corporate filings
    Copyright (C) 2017  Alexander Ions

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
import time
from datetime import datetime
import copy
import os
from abc import ABCMeta
import multiprocessing as mp
import re

from .utils import search_terms as master_search_terms
from .utils import args, logger

class Document(object):
    __metaclass__ = ABCMeta

    def __init__(self, file_path, doc_text, extraction_method):
        self._file_path = file_path
        self.doc_text = doc_text
        self.extraction_method = extraction_method
        self.log_cache = []

    def get_excerpt(self, input_text, form_type, metadata_master,
                    skip_existing_excerpts):
        """

        :param input_text:
        :param form_type:
        :param metadata_master:
        :param skip_existing_excerpts:
        :return:
        """
        start_time = time.process_time()
        self.prepare_text()
        prep_time = time.process_time() - start_time
        file_name_root = metadata_master.metadata_file_name

        section_search_terms_with_notes = []
        for section_search_terms in master_search_terms[form_type]:
            section_name = section_search_terms['itemname']
            if section_name != "Notes":
                section_search_terms_with_notes.append(section_search_terms)
            else:
                max_n = self.get_note_n()
                for i in range(1, max_n+1):
                    new_section_search_terms = copy.copy(section_search_terms) #section_search_terms.copy()
                    new_section_search_terms["itemname"] = f"Note{i}"
                    search_pairs = section_search_terms[self.search_terms_type()]
                    new_search_pairs = self.transfrom_note_search_pair(i, max_n, search_pairs)
                    new_section_search_terms[self.search_terms_type()] = new_search_pairs
                    section_search_terms_with_notes.append(new_section_search_terms)


        for section_search_terms_with_note in section_search_terms_with_notes:
            start_time = time.process_time()
            metadata = copy.copy(metadata_master)
            warnings = []
            section_name = section_search_terms_with_note['itemname']
            section_output_path = file_name_root + '_' + section_name
            txt_output_path = section_output_path + '_excerpt.txt'
            metadata_path = section_output_path + '_metadata.json'
            failure_metadata_output_path = section_output_path + '_failure.json'

            search_pairs = section_search_terms_with_note[self.search_terms_type()]
            text_extract, extraction_summary, start_text, end_text, warnings = \
                self.extract_section(search_pairs)
            time_elapsed = time.process_time() - start_time
            metadata.section_name = section_name
            if start_text:
                start_text = start_text.replace('\"', '\'')
            if end_text:
                end_text = end_text.replace('\"', '\'')
            metadata.endpoints = [start_text, end_text]
            metadata.warnings = warnings
            metadata.time_elapsed = round(prep_time + time_elapsed, 1)
            metadata.section_end_time = str(datetime.utcnow())
            if text_extract:
                if args.remove_short_line:
                    cleaned_text, section_n_table_removed = self.remove_short_single_line(text_extract)
                    metadata.section_n_table_removed = section_n_table_removed
                else:
                    cleaned_text = text_extract
                # success: save the excerpt file
                metadata.section_n_characters = len(cleaned_text)
                metadata.section_n_words = len(cleaned_text.split())
                with open(txt_output_path, 'w', encoding='utf-8',
                          newline='\n') as txt_output:
                    txt_output.write(cleaned_text)
                log_str = ': '.join(['SUCCESS Saved file for',
                                         section_name, txt_output_path])
                self.log_cache.append(('DEBUG', log_str))
                try:
                    os.remove(failure_metadata_output_path)
                except:
                    pass
                metadata.output_file = txt_output_path
                metadata.metadata_file_name = metadata_path
                metadata.save_to_json(metadata_path)
            else:
                log_str = ': '.join(['No excerpt located for ',
                                         section_name, metadata.sec_index_url])
                self.log_cache.append(('WARNING', log_str))
                try:
                    os.remove(metadata_path)
                except:
                    pass
                metadata.metadata_file_name = failure_metadata_output_path
                metadata.save_to_json(failure_metadata_output_path)
            if args.write_sql:
                metadata.save_to_db()
        return(self.log_cache)

    def transfrom_note_search_pair(self, i, max_n, search_pairs):
        pairs = []
        for pair in search_pairs:
            new_pair = copy.copy(pair) #pair.copy()
            new_pair['start'] = pair['start'].replace('-', f'{i}')
            if i != max_n:
                new_pair['end'] = pair['end'].replace('-', f'{i+1}')
            else:
                new_pair['end'] = "\n\s(?:PART.{,40})?Item\s9.{,10}Changes\sin\sand\sDisagreements\sWith.{,99}?\n"
            pairs.append(new_pair)
        return pairs

    def get_note_n(self):
        pattern =  'Note\s\d+'
        note_search = re.findall(pattern,
                            self.doc_text,
                            re.DOTALL | re.IGNORECASE)
        max_n = 0
        for note in note_search:
            try:
                n = int(note.split(" ")[1])
                if n > max_n:
                    max_n = n
            except Exception as e:
                print(f"Failed to get note number with {note}. {e}")

        return max_n

    def remove_short_single_line(self, text):
        
        orignal_text = copy.copy(text)

        section_n_table_removed = self.count_table_number(orignal_text)

        # remove page number, etc.
        patterns = ['^\s*([0-9]+\s*)+$', '^\s*(\[DATA_TABLE_REMOVED\]+\s*)+$', '^\s*(Table of Contents+\s*)+$']
        for pattern in patterns:
            orignal_text = re.sub(pattern, '', orignal_text, flags=re.M)

        orignal_text = re.sub(r'\n+', '\n\n', orignal_text).strip()
        return orignal_text, section_n_table_removed

    def count_table_number(self, text):
        return len(re.findall('\[DATA_TABLE_REMOVED\]', text))

    def prepare_text(self):
        # handled in child classes
        pass
