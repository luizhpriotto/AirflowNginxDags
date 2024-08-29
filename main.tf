terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.0.1"
    }
  }
}

provider "google" {
  project = "estudos-gcp-433522"
  region  = "US"
}

resource "google_container_cluster" "primary" {
  name     = "my-gke-cluster"
  location = "us-central1-f"

  # We can't create a cluster with no node pool defined, but we want to only use
  # separately managed node pools. So we create the smallest possible default
  # node pool and immediately delete it.
  remove_default_node_pool = true
  initial_node_count       = 1
  network = "k8s"
  subnetwork = "k8s"
  deletion_protection = false
}

resource "google_container_node_pool" "primary_preemptible_nodes" {
  name       = "my-node-pool"
  location = "us-central1-f"
  cluster    = google_container_cluster.primary.name
  node_count = 2


  node_config {
    preemptible  = true
    machine_type = "e2-medium"
    disk_size_gb = "60"
    disk_type = "pd-standard"
  }
}