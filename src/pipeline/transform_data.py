"""
módulo de transformação de dados para o pipeline etl.

este módulo é responsável por todas as transformações aplicadas aos dados brutos
da api de produtos, incluindo renomeação de campos, normalização de valores,
conversão de tipos, e adição de metadados de etl. assume-se que a api
não fornece um campo 'id' para os produtos; product_id será none inicialmente.
"""

# --- bibliotecas ---
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timezone
import copy
import re

# --- configuração de logging do módulo
logger = logging.getLogger(__name__)

# --- classe ---
class Transform:
    def __init__(self):
        """
        inicializa a classe de transformação de dados.
        """
        logger.info("instância de transformador de dados criada.")

    def _rename_and_select_fields(self, product_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        seleciona os campos desejados e os renomeia para o esquema de destino.
        cria novos dicionários para cada produto transformado.
        """
        logger.debug(f"selecionando e renomeando campos para {len(product_list)} produtos.")
        transformed_list = []

        for raw_product in product_list:
            field_map = {
                "Produto": "product_name",
                "Categoria do Produto": "category_name",
                "Preço": "price_original_str",
                "Frete": "shipping_cost_original_str",
                "Data da Compra": "purchase_date_str",
                "Vendedor": "seller_name",
                "Local da compra": "purchase_location_code",
                "Avaliação da compra": "purchase_rating_original",
                "Tipo de pagamento": "payment_type",
                "Quantidade de parcelas": "installments_quantity_original",
                "lat": "latitude_original",
                "lon": "longitude_original"
            }
            
            transformed_product = {}

            raw_id_value = raw_product.get("id")
            if raw_id_value == "" or raw_id_value is None: 
                transformed_product["product_id"] = None
                # logger.debug(f"id da api era '{raw_id_value}', product_id definido como none para produto original: {raw_product.get('Produto')}")
            else:
                transformed_product["product_id"] = str(raw_id_value)
                # logger.debug(f"id da api '{raw_id_value}' mapeado para product_id para produto original: {raw_product.get('Produto')}")

            for original_key, new_key in field_map.items():
                transformed_product[new_key] = raw_product.get(original_key)
            
            transformed_list.append(transformed_product)
        return transformed_list

    def _normalize_text_fields(self, product_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        normaliza campos de texto (converte para minúsculas e remove espaços extras).
        modifica os dicionários in-place na lista fornecida.
        """
        logger.debug(f"normalizando campos de texto para {len(product_list)} produtos.")
        fields_to_normalize = [
            "product_name", "category_name", "seller_name",
            "purchase_location_code", "payment_type"
        ]
        for product in product_list:
            for field in fields_to_normalize:
                if field in product and product[field] is not None:
                    try:
                        product[field] = str(product[field]).lower().strip()
                    except Exception as e:
                        # usa o product_id (que já deve existir após _rename_and_select_fields) ou product_name para identificação
                        product_identifier = product.get('product_id', product.get('product_name', 'desconhecido'))
                        logger.warning(f"não foi possível normalizar o campo '{field}' para o produto '{product_identifier}': {e}. valor: {product[field]}")
        return product_list

    def _convert_numeric_fields(self, product_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        converte campos numéricos para os tipos corretos (int, float),
        incluindo a conversão de preço e frete para centavos (inteiros).
        modifica os dicionários in-place na lista fornecida.
        """
        logger.debug(f"convertendo campos numéricos para {len(product_list)} produtos.")
        for product in product_list:
            product_identifier = product.get('product_id', product.get('product_name', 'id/nome desconhecido'))

            price_original = product.pop('price_original_str', None)
            if price_original is not None:
                try:
                    product['price_cents'] = int(float(price_original) * 100)
                except (ValueError, TypeError):
                    logger.warning(f"preço inválido '{price_original}' para produto '{product_identifier}'. definido como none.")
                    product['price_cents'] = None
            else:
                product['price_cents'] = None

            shipping_original = product.pop('shipping_cost_original_str', None)
            if shipping_original is not None:
                cleaned_shipping_str = str(shipping_original)
                match = re.search(r'(\d+\.?\d*|\.\d+)', cleaned_shipping_str)
                if match:
                    valid_numeric_part = match.group(1)
                    try:
                        product['shipping_cost_cents'] = int(float(valid_numeric_part) * 100)
                    except (ValueError, TypeError):
                        logger.warning(f"custo de frete inválido '{shipping_original}' (parte numérica: '{valid_numeric_part}') para produto '{product_identifier}'. definido como none.")
                        product['shipping_cost_cents'] = None
                else:
                    logger.warning(f"não foi possível extrair valor numérico do frete '{shipping_original}' para produto '{product_identifier}'. definido como none.")
                    product['shipping_cost_cents'] = None
            else:
                product['shipping_cost_cents'] = None

            rating_original = product.pop('purchase_rating_original', None)
            if rating_original is not None:
                try:
                    product['purchase_rating'] = int(rating_original)
                except (ValueError, TypeError):
                    logger.warning(f"avaliação da compra inválida '{rating_original}' para produto '{product_identifier}'. definido como none.")
                    product['purchase_rating'] = None
            else:
                product['purchase_rating'] = None

            installments_original = product.pop('installments_quantity_original', None)
            if installments_original is not None:
                try:
                    product['installments_quantity'] = int(installments_original)
                except (ValueError, TypeError):
                    logger.warning(f"quantidade de parcelas inválida '{installments_original}' para produto '{product_identifier}'. definido como none.")
                    product['installments_quantity'] = None
            else:
                product['installments_quantity'] = None
            
            for geo_field_original, geo_field_target in [('latitude_original', 'latitude'), ('longitude_original', 'longitude')]:
                geo_value = product.pop(geo_field_original, None)
                if geo_value is not None:
                    try:
                        product[geo_field_target] = float(geo_value)
                    except (ValueError, TypeError):
                        logger.warning(f"valor de '{geo_field_original}' inválido '{geo_value}' para produto '{product_identifier}'. campo '{geo_field_target}' definido como none.")
                        product[geo_field_target] = None
                else:
                    product[geo_field_target] = None
        return product_list

    def _convert_dates(self, product_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        converte o campo 'purchase_date_str' para objeto date do python.
        modifica os dicionários in-place na lista fornecida.
        """
        logger.debug(f"convertendo campos de data para {len(product_list)} produtos.")
        date_field_original = 'purchase_date_str'
        date_field_target = 'purchase_date'
        date_format_str = '%d/%m/%Y' # formato da sua amostra de dados

        for product in product_list:
            product_identifier = product.get('product_id', product.get('product_name', 'id/nome desconhecido'))
            date_str_value = product.pop(date_field_original, None)

            if date_str_value is not None and isinstance(date_str_value, str) and date_str_value.strip():
                try:
                    product[date_field_target] = datetime.strptime(date_str_value, date_format_str).date()
                except ValueError:
                    logger.warning(
                        f"não foi possível converter data '{date_str_value}' no campo '{date_field_original}' "
                        f"com formato '{date_format_str}' para o produto '{product_identifier}'. campo '{date_field_target}' definido como none."
                    )
                    product[date_field_target] = None
            else:
                product[date_field_target] = None # valor original none, string vazia, ou não string
        return product_list

    def _add_etl_metadata(self, product_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        adiciona metadados de etl, como um timestamp de carregamento.
        modifica os dicionários in-place na lista fornecida.
        """
        logger.debug(f"adicionando metadados de etl para {len(product_list)} produtos.")
        etl_timestamp = datetime.now(timezone.utc)
        for product in product_list:
            product['etl_load_timestamp'] = etl_timestamp
        return product_list

    def _ensure_final_structure_and_defaults(self, product_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        garante que todos os campos finais esperados existam, preenchendo com defaults
        se ausentes ou none após transformações.
        modifica os dicionários in-place na lista fornecida.
        """
        logger.debug(f"garantindo estrutura final e defaults para {len(product_list)} produtos.")
        final_schema_defaults = {
            "product_id": None,
            "product_name": "nome indisponível",
            "category_name": "outros",
            "price_cents": None,
            "shipping_cost_cents": None,
            "purchase_date": None,
            "seller_name": "vendedor desconhecido",
            "purchase_location_code": "n/a",
            "purchase_rating": None,
            "payment_type": "não especificado",
            "installments_quantity": None,
            "latitude": None,
            "longitude": None,
            "etl_load_timestamp": None # será preenchido por _add_etl_metadata, mas bom ter aqui para estrutura
        }

        for product in product_list:
            for field, default_value in final_schema_defaults.items():
                if product.get(field) is None: # cobre chave ausente ou chave com valor none
                    product[field] = default_value
        return product_list

    def transform_data(self, raw_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        executa uma sequência de transformações nos dados brutos da api.
        """
        if not raw_data_list:
            logger.warning("nenhum dado bruto fornecido para transformação.")
            return []

        logger.info(f"iniciando processo de transformação para {len(raw_data_list)} registros brutos.")

        try:
            # cópia profunda para não modificar os dicionários originais da lista de entrada
            data_to_transform = copy.deepcopy(raw_data_list)
            logger.debug("cópia profunda dos dados brutos realizada para transformação.")
        except Exception as e:
            logger.error(f"erro ao realizar cópia profunda dos dados: {e}. usando cópia superficial de cada item como fallback.", exc_info=True)
            # se deepcopy falhar, fazer uma cópia superficial de cada dicionário individualmente
            # para que a lista externa seja nova e os dicionários internos também sejam cópias (superficiais).
            data_to_transform = [item.copy() for item in raw_data_list]

        # etapa 1: selecionar e renomear campos. esta etapa cria uma nova lista de novos dicionários.
        transformed_list = self._rename_and_select_fields(data_to_transform)

        # as etapas seguintes modificam os dicionários na 'transformed_list' in-place.
        transformed_list = self._normalize_text_fields(transformed_list)
        transformed_list = self._convert_numeric_fields(transformed_list)
        transformed_list = self._convert_dates(transformed_list)
        
        # garantir estrutura final e defaults antes de adicionar o timestamp de etl,
        # pois etl_load_timestamp também está no schema de defaults.
        transformed_list = self._ensure_final_structure_and_defaults(transformed_list)

        # adicionar metadados de etl como última etapa de modificação de conteúdo.
        transformed_list = self._add_etl_metadata(transformed_list)
        
        # não há filtro final por product_id aqui; todos os registros processados são retornados.
        # a decisão de carregar ou não registros com product_id nulo pode ser feita na etapa de carga.
        final_valid_data = transformed_list

        logger.info(f"transformação concluída. {len(final_valid_data)} registros processados.")
        return final_valid_data