from search_index import search


def build_context(query, top_k=5):

    results = search(query, top_k=top_k)

    context_parts = []

    for i, result in enumerate(results, start=1):

        context_parts.append(
            f"""
DOCUMENT {i}

Similarity score: {result['similarity_score']}

{result['document_text']}
"""
        )

    context = "\n".join(context_parts)

    return context


if __name__ == "__main__":

    query = "Why are customers unhappy with delivery?"

    context = build_context(query, top_k=5)

    print("\nRAG CONTEXT")
    print("=" * 80)
    print(context)