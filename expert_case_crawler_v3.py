"""
EXPERT CLINICAL CASE CRAWLER V3 - OPTIMIZED
Tối ưu: 25 diseases quan trọng nhất + các loại CANCER
"""

import requests
import xml.etree.ElementTree as ET
import time
import csv
from datetime import datetime
import os
import re

class ExpertCaseCrawlerV3:
    def __init__(self, email="kiennct2711.it@gmail.com"):
        self.base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
        self.email = email
        
        # 25 DISEASES QUAN TRỌNG NHẤT cho Data Mining
        # Được chọn dựa trên: prevalence, clinical significance, data availability
        self.diseases = [
            # CANCERS (10 types - rất quan trọng!)
            'breast cancer',
            'lung cancer', 
            'colon cancer',
            'prostate cancer',
            'gastric cancer',
            'liver cancer',
            'pancreatic cancer',
            'ovarian cancer',
            'cervical cancer',
            'thyroid cancer',
            
            # INFECTIOUS DISEASES (5)
            'tuberculosis',
            'pneumonia',
            'sepsis',
            'meningitis',
            'covid-19',
            
            # CHRONIC DISEASES (5)
            'diabetes mellitus',
            'hypertension',
            'heart failure',
            'stroke',
            'chronic kidney disease',
            
            # EMERGENCY & CRITICAL (3)
            'myocardial infarction',
            'pulmonary embolism',
            'acute respiratory failure',
            
            # COMMON CONDITIONS (2)
            'asthma',
            'epilepsy'
        ]
        
        self.csv_file = f"raw_cases_optimized_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.csv_headers = [
            'case_id', 'pmid', 'pmcid', 'disease_category', 'title',
            'patient_age', 'patient_gender',
            'chief_complaint', 'symptoms_raw', 'physical_exam_raw',
            'lab_results_raw', 'imaging_raw', 'diagnosis_raw',
            'treatment_raw', 'outcome_raw', 'full_clinical_text',
            'publication_year', 'journal', 'authors', 'doi', 'url'
        ]
        
        self.stats = {'searched': 0, 'found': 0, 'no_pmc': 0, 'no_text': 0, 'saved': 0}
    
    def search_case_reports(self, disease, max_results=5000):
        """Search PubMed for case reports - FIXED QUERY"""
        # FIXED: Use simple query - works much better!
        # Strategy: "{disease} case report" finds 10x more results than Publication Type filter
        query = f'{disease} case report'
        
        params = {
            'db': 'pubmed',
            'term': query,
            'retmax': max_results,
            'retmode': 'json',
            'email': self.email,
            'sort': 'relevance'
        }
        
        try:
            response = requests.get(f"{self.base_url}esearch.fcgi", params=params, timeout=20)
            data = response.json()
            pmids = data.get('esearchresult', {}).get('idlist', [])
            count = int(data.get('esearchresult', {}).get('count', 0))
            return pmids, count
        except Exception as e:
            print(f"      Search error: {e}")
            return [], 0
    
    def get_pmc_id(self, pmid):
        """Convert PMID to PMCID - FASTER"""
        params = {'ids': pmid, 'format': 'json', 'email': self.email}
        
        try:
            response = requests.get('https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/', 
                                  params=params, timeout=15)
            data = response.json()
            records = data.get('records', [])
            if records and 'pmcid' in records[0]:
                return records[0]['pmcid'].replace('PMC', '')
        except:
            pass
        return None
    
    def fetch_pmc_xml(self, pmcid):
        """Fetch full XML from PMC - FASTER"""
        params = {'db': 'pmc', 'id': pmcid, 'retmode': 'xml', 'email': self.email}
        
        try:
            response = requests.get(f"{self.base_url}efetch.fcgi", params=params, timeout=20)
            if response.status_code == 200:
                return response.text
        except:
            pass
        return None
    
    def extract_text_recursive(self, elem):
        """Get all text from XML element recursively"""
        texts = []
        if elem.text:
            texts.append(elem.text.strip())
        for child in elem:
            texts.append(self.extract_text_recursive(child))
            if child.tail:
                texts.append(child.tail.strip())
        return ' '.join(filter(None, texts))
    
    def extract_raw_clinical_data(self, xml_text, pmid, pmcid, disease):
        """Extract RAW clinical data - OPTIMIZED"""
        try:
            root = ET.fromstring(xml_text)
            
            case_data = {
                'case_id': f"{disease.replace(' ', '_')}_{pmcid}",
                'pmid': pmid,
                'pmcid': f'PMC{pmcid}',
                'disease_category': disease,
                'url': f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmcid}/"
            }
            
            # Metadata
            title = root.find('.//article-title')
            case_data['title'] = self.extract_text_recursive(title) if title is not None else ''
            
            pub_year = root.find('.//pub-date//year')
            case_data['publication_year'] = pub_year.text if pub_year is not None else ''
            
            journal = root.find('.//journal-title')
            case_data['journal'] = journal.text if journal is not None else ''
            
            # Authors
            authors = []
            for author in root.findall('.//contrib[@contrib-type="author"]'):
                surname = author.find('.//surname')
                given = author.find('.//given-names')
                if surname is not None:
                    name = surname.text
                    if given is not None:
                        name += f' {given.text}'
                    authors.append(name)
            case_data['authors'] = '; '.join(authors[:5]) if authors else ''
            
            doi = root.find('.//article-id[@pub-id-type="doi"]')
            case_data['doi'] = doi.text if doi is not None else ''
            
            # Initialize clinical fields
            case_data.update({
                'patient_age': '',
                'patient_gender': '',
                'chief_complaint': '',
                'symptoms_raw': '',
                'physical_exam_raw': '',
                'lab_results_raw': '',
                'imaging_raw': '',
                'diagnosis_raw': '',
                'treatment_raw': '',
                'outcome_raw': '',
                'full_clinical_text': ''
            })
            
            all_clinical_text = []
            body = root.find('.//body')
            
            if body is not None:
                for sec in body.findall('.//sec'):
                    title_elem = sec.find('.//title')
                    section_title = self.extract_text_recursive(title_elem).lower() if title_elem is not None else ''
                    
                    # Get all paragraphs
                    section_text = []
                    for p in sec.findall('.//p'):
                        p_text = self.extract_text_recursive(p)
                        if len(p_text) > 20:
                            section_text.append(p_text)
                    
                    full_section = ' '.join(section_text)
                    
                    if not full_section:
                        continue
                    
                    # Classify sections
                    if any(kw in section_title for kw in ['case', 'patient', 'presentation', 'history']):
                        all_clinical_text.append(full_section)
                        
                        if not case_data['patient_age']:
                            age = self._find_age(full_section)
                            if age:
                                case_data['patient_age'] = age
                        
                        if not case_data['patient_gender']:
                            gender = self._find_gender(full_section)
                            if gender:
                                case_data['patient_gender'] = gender
                        
                        if not case_data['chief_complaint']:
                            case_data['chief_complaint'] = full_section[:5000]
                    
                    if any(kw in section_title for kw in ['symptom', 'sign', 'clinical feature', 'manifestation']):
                        case_data['symptoms_raw'] += ' ' + full_section
                        all_clinical_text.append(full_section)
                    
                    if any(kw in section_title for kw in ['physical exam', 'examination']):
                        case_data['physical_exam_raw'] += ' ' + full_section
                        all_clinical_text.append(full_section)
                    
                    if any(kw in section_title for kw in ['lab', 'laboratory', 'blood test', 'investigation']):
                        case_data['lab_results_raw'] += ' ' + full_section
                        all_clinical_text.append(full_section)
                    
                    if any(kw in section_title for kw in ['imaging', 'radiology', 'x-ray', 'ct', 'mri']):
                        case_data['imaging_raw'] += ' ' + full_section
                        all_clinical_text.append(full_section)
                    
                    if any(kw in section_title for kw in ['diagnosis', 'diagnostic']):
                        case_data['diagnosis_raw'] += ' ' + full_section
                        all_clinical_text.append(full_section)
                    
                    if any(kw in section_title for kw in ['treatment', 'management', 'therapy']):
                        case_data['treatment_raw'] += ' ' + full_section
                        all_clinical_text.append(full_section)
                    
                    if any(kw in section_title for kw in ['outcome', 'follow', 'recovery', 'prognosis']):
                        case_data['outcome_raw'] += ' ' + full_section
                        all_clinical_text.append(full_section)
            
            # Full clinical text
            case_data['full_clinical_text'] = ' '.join(all_clinical_text)
            
            # Clean up whitespace
            for key in case_data:
                if isinstance(case_data[key], str):
                    case_data[key] = ' '.join(case_data[key].split())
            
            # Validate
            if len(case_data['full_clinical_text']) < 100:
                return None
            
            return case_data
            
        except:
            return None
    
    def _find_age(self, text):
        """Extract age"""
        patterns = [
            r'(\d+)[\s-]year[\s-]old',
            r'aged?\s+(\d+)',
            r'(\d+)[\s-]y/?o\b',
            r'age[:\s]+(\d+)',
            r'\b(\d+)\s*years?\s+of\s+age'
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                age = int(match.group(1))
                if 0 < age < 120:
                    return str(age)
        return ''
    
    def _find_gender(self, text):
        """Extract gender"""
        text_lower = text.lower()
        male_score = sum([
            text_lower.count('male patient'),
            text_lower.count(' man '),
            text_lower.count(' boy '),
            text_lower.count(' he '),
            text_lower.count(' his ')
        ])
        female_score = sum([
            text_lower.count('female patient'),
            text_lower.count(' woman '),
            text_lower.count(' girl '),
            text_lower.count(' she '),
            text_lower.count(' her ')
        ])
        if male_score > female_score and male_score > 0:
            return 'Male'
        elif female_score > male_score and female_score > 0:
            return 'Female'
        return ''
    
    def save_to_csv(self, case_data):
        """Save immediately to CSV"""
        file_exists = os.path.exists(self.csv_file)
        with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.csv_headers)
            if not file_exists:
                writer.writeheader()
            writer.writerow(case_data)
    
    def crawl(self, target=10000, max_per_disease=5000):
        """Main crawl function - OPTIMIZED"""
        print("="*90)
        print(f" Target: {target} raw clinical case reports")
        print(f" Strategy: {len(self.diseases)} CRITICAL diseases (includes 10 CANCERS!)")
        print(f" Output: {self.csv_file}")
        print(f" Optimizations: Faster timeouts, 25 diseases instead of 40")
        print("="*90)
        
        total_saved = 0
        start_time = time.time()
        
        for idx, disease in enumerate(self.diseases, 1):
            if total_saved >= target:
                print(f"\n✅ Target reached: {target} cases!")
                break
            
            disease_start = time.time()
            print(f"\n[{idx}/{len(self.diseases)}] 🔍 {disease.upper()}")
            
            # Search PubMed
            pmids, count = self.search_case_reports(disease, max_per_disease)
            print(f"   Found {count} total, processing {len(pmids)}...")
            self.stats['searched'] += len(pmids)
            
            if not pmids:
                continue
            
            disease_saved = 0
            
            for i, pmid in enumerate(pmids, 1):
                if total_saved >= target or disease_saved >= max_per_disease:
                    break
                
                # Progress indicator every 10
                if i % 10 == 0:
                    elapsed = time.time() - disease_start
                    rate = i / elapsed if elapsed > 0 else 0
                    print(f"   [{i}/{len(pmids)}] Rate: {rate:.1f} articles/sec, Saved: {disease_saved}")
                
                # Get PMC ID
                pmcid = self.get_pmc_id(pmid)
                if not pmcid:
                    self.stats['no_pmc'] += 1
                    continue
                
                self.stats['found'] += 1
                
                # Fetch full text
                xml = self.fetch_pmc_xml(pmcid)
                if not xml:
                    continue
                
                # Extract raw clinical data
                case_data = self.extract_raw_clinical_data(xml, pmid, pmcid, disease)
                if not case_data:
                    self.stats['no_text'] += 1
                    continue
                
                # Save to CSV
                self.save_to_csv(case_data)
                disease_saved += 1
                total_saved += 1
                self.stats['saved'] += 1
                
                time.sleep(0.35)  # Rate limit
            
            disease_time = time.time() - disease_start
            print(f"   ✅ Disease: {disease_saved} cases in {disease_time/60:.1f} min")
            time.sleep(1)
        
        total_time = time.time() - start_time
        
        print("\n" + "="*90)
        print(" FINAL STATISTICS")
        print("="*90)
        print(f"  Total time: {total_time/3600:.2f} hours ({total_time/60:.1f} minutes)")
        print(f" PMIDs searched: {self.stats['searched']}")
        print(f" With PMC full text: {self.stats['found']} ({self.stats['found']/max(self.stats['searched'],1)*100:.1f}%)")
        print(f" With clinical data: {self.stats['saved']} ({self.stats['saved']/max(self.stats['found'],1)*100:.1f}%)")
        print(f" No PMC: {self.stats['no_pmc']}")
        print(f" No clinical text: {self.stats['no_text']}")
        print(f"\n TOTAL SAVED: {total_saved} raw clinical case reports")
        print(f" OUTPUT FILE: {self.csv_file}")
        print(f" Average rate: {total_saved/(total_time/3600):.0f} cases/hour")
        print("="*90)
        
        return total_saved


if __name__ == "__main__":
    print(" Starting OPTIMIZED Expert Clinical Case Crawler V3...")
    print(" Includes 10 types of CANCER + 15 other critical diseases")
    print()
    
    crawler = ExpertCaseCrawlerV3(email="student@university.edu")
    total = crawler.crawl(target=10000, max_per_disease=5000)
    
    print(f"\n✨ Crawling completed!")
    print(f" Total: {total} raw clinical cases with CANCER data included!")
    print(f"\n Next steps:")
    print(f"   import pandas as pd")
    print(f"   df = pd.read_csv('{crawler.csv_file}')")
    print(f"   print(df['disease_category'].value_counts())  # Check distribution")
