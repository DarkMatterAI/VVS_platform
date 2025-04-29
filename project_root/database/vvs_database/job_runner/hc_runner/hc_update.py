import numpy as np
from typing import List, Tuple, Dict 
from collections import defaultdict 

from vvs_database.gradients import distance_to_update
from vvs_database.schemas.hc_schemas import HCSearchIteration, UpdateType
from vvs_database.schemas.internal_schemas import GradientEmbedding, InternalItem

def get_update_results(search_iteration: HCSearchIteration
                      ) -> Tuple[List[InternalItem], InternalItem]:
    
    results = search_iteration.get_results(only_valid=True)
    results = sorted(results, key=lambda x: x.score.score)
    top_result = results[-1]
    
    # subset before deduplication if necessary
    if search_iteration.update_params.update_type == 'group_update':
        results = [i for i in results if i.query_group == top_result.query_group]
        
    # deduplicate 
    unique_results = {i.item_data.item_id:i for i in results}
    results = list(unique_results.values())
    
    return results, top_result

def get_update_dict(results: List[InternalItem],
                    top_result: InternalItem,
                   ) -> Tuple[Dict[int, List[float]], 
                              Dict[int, List[List[float]]]]:
    
    if top_result.update_embedding is not None:
        query_dict = {0 : top_result.update_embedding.embedding}
        embedding_dict = {0 : [i.update_embedding.embedding for i in results]}
    else:
        parents = [p.to_parent() for p in top_result.assembly_data.parents]
        query_dict = {p.assembly_index : p.embedding.embedding for p in parents}
        embedding_dict = defaultdict(list)
        for result in results:
            for p in result.assembly_data.parents:
                p = p.to_parent()
                embedding_dict[p.assembly_index].append(p.embedding.embedding)
                
    return query_dict, embedding_dict


def top_1_update(search_iteration: HCSearchIteration
                ) -> List[GradientEmbedding]:
    
    results, top_result = get_update_results(search_iteration)
    
    # compute advantages
    scores = np.array([i.score.score for i in results])
    advantages = (scores - scores.mean()) / (scores.std() + 1e-8)
    
    # get update dicts
    query_dict, embedding_dict = get_update_dict(results, top_result)
    previous_dict = {i.assembly_index:i for i in search_iteration.query_embeddings}
    include_fields = ['plugin_id', 'plugin_name', 'learning_rates', 'assembly_index']
    
    # select gradient function
    grad_func = distance_to_update[search_iteration.update_params.distance_metric]
    
    # create output
    output = []
    for assembly_index in previous_dict.keys():
        previous_query = previous_dict[assembly_index]
        query_embedding = np.array(query_dict[assembly_index])[None]
        embeddings = np.array(embedding_dict[assembly_index])
        grad = grad_func(query_embedding, embeddings, advantages)
        output.append(GradientEmbedding(**previous_query.model_dump(include=include_fields),
                                        embedding=query_embedding[0].tolist(),
                                        gradient=grad.tolist()))
    return output 