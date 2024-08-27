# File: ./app.py

import streamlit as st
from helpers import (
    CollectionName,
    STREAMLIT_STYLING,
    connect_to_weaviate,
    get_top_companies,
    search_objects,
    get_pprof_results,
    rag,
)
import plotly.graph_objs as go
from datetime import datetime
import time
import re

st.set_page_config(page_title="Scalable RAG with Weaviate", layout="wide")

st.markdown(STREAMLIT_STYLING, unsafe_allow_html=True)

with connect_to_weaviate() as client:
    st.markdown(
        "<div class='stHeader'><h1>Scalable RAG with Weaviate</h1></div>",
        unsafe_allow_html=True,
    )

    collection_name = CollectionName.SUPPORTCHAT
    collection = client.collections.get(collection_name)

    config = collection.config.get()
    mt_enabled = config.multi_tenancy_config.enabled

    # Create two main columns
    col1, col2 = st.columns([2, 1], gap="large")

    with col1:
        st.markdown("### Customer support analysis")

        top_companies = get_top_companies(collection)
        if len(top_companies) > 0:
            top_companies_str = ", ".join(
                [f"**{company.value}** ({company.count})" for company in top_companies]
            )
            st.info(body="Example source accounts: " + top_companies_str, icon="ℹ️")

        # ===== Search inputs =====

        st.markdown("#### Search")

        with st.container(border=True):
            input_c1, input_c2 = st.columns(2, gap="large")

        with input_c1:
            query = st.text_input("Query", value="returns")
            limit = st.number_input("Limit", value=5, min_value=1, max_value=20)

        with input_c2:
            company_filter = st.text_input(
                label="Filter by company account name (*: wildcards)", value="*amazon*"
            )
            search_type = st.radio(
                label="Search type",
                options=["Hybrid", "Vector", "Keyword"],
                horizontal=True,
                index=0,
            )

        # ===== Search and display results =====

        st.markdown("#### Results")

        search_response = search_objects(
            collection, query, company_filter, limit, search_type
        )

        st.markdown(f"For query: `{query}`")
        with st.container(height=250):
            for o in search_response.objects:
                with st.expander(
                    f"**{o.properties['company_author']}**: {o.properties['text'][:50]}..."
                ):
                    st.write(f"Dialog ID: {o.properties['dialogue_id']}")
                    st.write(f"Created at: {o.properties['created_at']}")
                    st.write(f"Full text: {o.properties['text']}")

        # ===== RAG =====

        # Using claudette (https://claudette.answer.ai/)
        # API key will be read from the environment variable ANTHROPIC_API_KEY

        if len(search_response.objects) > 0:
            rag_query = st.text_area(
                label="What should we do with the search results?",
            )

            if st.button("Generate response"):
                with st.spinner("Generating response..."):
                    context = "\n\n".join(
                        [
                            f'Comany account: <company>{o.properties["company_author"]}</company> <converstion>{o.properties["text"]}</converstion>'
                            for o in search_response.objects
                        ]
                    )
                    rag_responses = rag(rag_query, context, provider="claude")

                    if rag_responses:
                        with st.container(height=250, border=True):
                            for c in rag_responses:
                                st.write(c)

    with col2:
        st.markdown("### Cluster statistics")

        with st.container(border=True):
            if mt_enabled:
                tenants = collection.tenants.get()
                st.metric(label="Tenant count", value=len(tenants))
            else:
                obj_count = collection.aggregate.over_all(total_count=True).total_count
                st.metric(label="Object count", value=obj_count)

        with st.container(border=True):
            node_data = client.cluster.nodes(output="verbose")
            st.metric(label="Nodes", value=len(node_data))

        # with st.container(border=True):
        #     result = get_pprof_results()

        #     if result.returncode == 0:
        #         match = re.search(
        #             r"Showing nodes accounting for (\d+\.?\d*)MB, (\d+\.?\d*)% of (\d+\.?\d*)MB total",
        #             result.stdout,
        #         )
        #         if match:
        #             total_mb = float(match.group(3))
        #             st.metric(label="Memory usage", value=f"{total_mb:.1f} MB")
        #     else:
        #         st.error("Error running pprof")

        with st.container(border=True):

            @st.fragment(run_every=2)
            def update_memory_chart():
                # Initialize data for the plot
                if "memory_data" not in st.session_state:
                    st.session_state.memory_data = {"time": [], "usage": []}

                # Function to update memory data
                def update_memory_data():
                    result = get_pprof_results()
                    if result.returncode == 0:
                        match = re.search(
                            r"Showing nodes accounting for (\d+\.?\d*)MB, (\d+\.?\d*)% of (\d+\.?\d*)MB total",
                            result.stdout,
                        )
                        if match:
                            total_mb = float(match.group(3))
                            current_time = datetime.now().strftime("%H:%M:%S")
                            st.session_state.memory_data["time"].append(current_time)
                            st.session_state.memory_data["usage"].append(total_mb)

                            # Keep only the last 50 data points
                            if len(st.session_state.memory_data["time"]) > 50:
                                st.session_state.memory_data[
                                    "time"
                                ] = st.session_state.memory_data["time"][-50:]
                                st.session_state.memory_data[
                                    "usage"
                                ] = st.session_state.memory_data["usage"][-50:]

                # Update memory data
                update_memory_data()

                # Create and display the plot
                fig = go.Figure(
                    data=go.Scatter(
                        x=st.session_state.memory_data["time"],
                        y=st.session_state.memory_data["usage"],
                        mode="lines+markers",
                    ),
                )
                fig.update_layout(
                    title="Memory Usage Over Time",
                    xaxis_title="Time",
                    yaxis_title="Memory Usage (MB)",
                    height=300,
                    margin=dict(l=50, r=10, t=30, b=30),
                    xaxis=dict(
                        tickangle=45, tickmode="linear", dtick=5  # Show every 5th tick
                    ),
                )
                fig.update_xaxes(fixedrange=True)  # Disable x-axis zoom
                fig.update_yaxes(fixedrange=True)  # Disable y-axis zoom
                st.plotly_chart(
                    fig, use_container_width=True, config={"displayModeBar": False}
                )

                # Rerun this fragment after a delay
                time.sleep(2)

            update_memory_chart()

        st.markdown("### Under the hood")
        with st.expander("Weaviate configuration (JSON)"):
            with st.container(height=300):
                st.json(config.to_dict())